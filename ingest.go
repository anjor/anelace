package anelace

import (
	"encoding/binary"
	"fmt"
	"github.com/anjor/anelace/internal/block"
	"github.com/anjor/anelace/internal/chunker"
	"github.com/anjor/anelace/internal/constants"
	"github.com/anjor/anelace/internal/util/encoding"
	"github.com/anjor/anelace/internal/util/text"
	"github.com/anjor/anelace/internal/util/zcpstring"
	"io"
	"log"
	"sync/atomic"
	"time"

	"github.com/ipfs/go-qringbuf"
)

// SANCHECK: not sure if any of these make sense, nor have I measured the cost
const (
	carQueueSize = 2048
)

const (
	ErrorString = IngestionEventType(iota)
	NewRootJsonl
)

type carUnit struct {
	_      constants.Incomparabe
	hdr    *anlblock.Header
	region *qringbuf.Region
}

type seenBlocks map[[seenHashSize]byte]uniqueBlockStats
type seenRoots map[[seenHashSize]byte]seenRoot

type IngestionEvent struct {
	_    constants.Incomparabe
	Type IngestionEventType
	Body string
}
type IngestionEventType int

// SANCHECK - we probably want some sort of timeout or somesuch here...
func (anl *Anelace) maybeSendEvent(t IngestionEventType, s string) {
	if anl.externalEventBus != nil {
		anl.externalEventBus <- IngestionEvent{Type: t, Body: s}
	}
}

var preProcessTasks, postProcessTasks func(anl *Anelace)

func (anl *Anelace) ProcessReader(inputReader io.Reader, optionalEventChan chan<- IngestionEvent) (err error) {

	var t0 time.Time

	defer func() {

		// a little helper to deal with error stack craziness
		deferErrors := make(chan error, 1)

		// if we are already in error - just put it on the channel
		// we already sent the event earlier
		if err != nil {
			deferErrors <- err
		}

		// keep sending out events but keep at most 1 error to return synchronously
		addErr := func(e error) {
			if e != nil {
				anl.maybeSendEvent(ErrorString, e.Error())
				select {
				case deferErrors <- e:
				default:
				}
			}
		}

		// we need to wait all crunching to complete, then shutdown emitter, then measure/return
		anl.asyncWG.Wait()

		// we are writing data: need to wait/close things
		if anl.carDataQueue != nil {
			close(anl.carDataQueue)     // signal data-write stop
			addErr(<-anl.carWriteError) // wait for data-write stop
		}

		if err == nil && len(deferErrors) > 0 {
			err = <-deferErrors
		}

		if postProcessTasks != nil {
			postProcessTasks(anl)
		}

		anl.qrb = nil
		if anl.externalEventBus != nil {
			close(anl.externalEventBus)
		}

		anl.statSummary.SysStats.ElapsedNsecs = time.Since(t0).Nanoseconds()
	}()

	anl.externalEventBus = optionalEventChan
	defer func() {
		if err != nil {

			var buffered int
			if anl.qrb != nil {
				anl.qrb.Lock()
				buffered = anl.qrb.Buffered()
				anl.qrb.Unlock()
			}

			err = fmt.Errorf(
				"failure at byte offset %s of sub-stream #%d with %s bytes buffered/unprocessed: %s",
				text.Commify64(anl.curStreamOffset),
				anl.statSummary.Streams,
				text.Commify(buffered),
				err,
			)

			anl.maybeSendEvent(ErrorString, err.Error())
		}
	}()

	if preProcessTasks != nil {
		preProcessTasks(anl)
	}
	t0 = time.Now()

	anl.qrb, err = qringbuf.NewFromReader(inputReader, qringbuf.Config{
		// MinRegion must be twice the maxchunk, otherwise chunking chains won't work (hi, Claude Shannon)
		MinRegion:   2 * constants.MaxLeafPayloadSize,
		MinRead:     anl.cfg.RingBufferMinRead,
		MaxCopy:     2 * constants.MaxLeafPayloadSize, // SANCHECK having it equal to the MinRegion may be daft...
		BufferSize:  anl.cfg.RingBufferSize,
		SectorSize:  anl.cfg.RingBufferSectSize,
		Stats:       &anl.statSummary.SysStats.Stats,
		TrackTiming: ((anl.cfg.StatsActive & statsRingbuf) == statsRingbuf),
	})
	if err != nil {
		return
	}

	// We got that far - got to write out the data portion prequel
	// .oO( The machine of a dream, such a clean machine
	//      With the pistons a pumpin', and the hubcaps all gleam )
	if anl.carDataWriter != nil {
		if _, err = io.WriteString(anl.carDataWriter, anlblock.NulRootCarHeader); err != nil {
			return
		}

		// start the async writer here, once we know nothing errorred
		anl.carDataQueue = make(chan carUnit, carQueueSize)
		anl.carWriteError = make(chan error, 1)
		go anl.backgroundCarDataWriter()
	}

	if (anl.cfg.StatsActive & statsBlocks) == statsBlocks {
		anl.seenBlocks = make(seenBlocks, 1024) // SANCHECK: somewhat arbitrary, but eh...
		anl.seenRoots = make(seenRoots, 32)
	}

	// use 64bits everywhere
	var substreamSize int64

	// outer stream loop: read() syscalls happen only here and in the qrb.collector()
	for {
		if anl.cfg.MultipartStream {

			err := binary.Read(
				inputReader,
				binary.BigEndian,
				&substreamSize,
			)
			anl.statSummary.SysStats.ReadCalls++

			if err == io.EOF {
				// no new multipart coming - bail
				break
			} else if err != nil {
				return fmt.Errorf(
					"error reading next 8-byte multipart substream size: %s",
					err,
				)
			}

			if substreamSize == 0 && anl.cfg.SkipNulInputs {
				continue
			}

			anl.statSummary.Streams++
			anl.curStreamOffset = 0
		}

		if anl.cfg.MultipartStream && substreamSize == 0 {
			// If we got here: cfg.ProcessNulInputs is true
			// Special case for a one-time zero-CID emission
			anl.streamAppend(nil)
		} else if err := anl.processStream(substreamSize); err != nil {
			if err == io.ErrUnexpectedEOF {
				return fmt.Errorf(
					"unexpected end of substream #%s after %s bytes (stream expected to be %s bytes long)",
					text.Commify64(anl.statSummary.Streams),
					text.Commify64(anl.curStreamOffset+int64(anl.qrb.Buffered())),
					text.Commify64(substreamSize),
				)
			} else if err != io.EOF {
				return err
			} else if anl.curStreamOffset == 0 && !anl.cfg.SkipNulInputs {
				// we did try to process a stream and ended up with an EOF + 0
				// emit a zero-CID like above
				anl.streamAppend(nil)
			}
		}

		if anl.generateRoots || anl.seenRoots != nil || anl.externalEventBus != nil {

			rootBlock := anl.collector.FlushState()

			var rootPayloadSize, rootDagSize uint64
			if rootBlock != nil {
				rootPayloadSize = rootBlock.SizeCumulativePayload()
				rootDagSize = rootBlock.SizeCumulativeDag()

				if anl.seenRoots != nil {
					anl.mu.Lock()

					var rootSeen bool
					if sk := seenKey(rootBlock); sk != nil {
						if _, rootSeen = anl.seenRoots[*sk]; !rootSeen {
							anl.seenRoots[*sk] = seenRoot{
								order: len(anl.seenRoots),
								cid:   rootBlock.Cid(),
							}
						}
					}

					anl.statSummary.Roots = append(anl.statSummary.Roots, rootStats{
						Cid:         anl.formattedCid(rootBlock),
						SizePayload: rootBlock.SizeCumulativePayload(),
						SizeDag:     rootBlock.SizeCumulativeDag(),
						Dup:         rootSeen,
					})

					anl.mu.Unlock()
				}
			}

			jsonl := fmt.Sprintf(
				"{\"event\":   \"root\", \"payload\":%12d, \"stream\":%7d, %-67s, \"wiresize\":%12d }\n",
				rootPayloadSize,
				anl.statSummary.Streams,
				fmt.Sprintf(`"cid":"%s"`, anl.formattedCid(rootBlock)),
				rootDagSize,
			)
			anl.maybeSendEvent(NewRootJsonl, jsonl)
			if rootBlock != nil && anl.cfg.emitters[emRootsJsonl] != nil {
				if _, err := io.WriteString(anl.cfg.emitters[emRootsJsonl], jsonl); err != nil {
					return fmt.Errorf("emitting '%s' failed: %s", emRootsJsonl, err)
				}
			}
		}

		// we are in EOF-state: if we are not expecting multiparts - we are done
		if !anl.cfg.MultipartStream {
			break
		}
	}

	return
}

func (anl *Anelace) backgroundCarDataWriter() {
	defer close(anl.carWriteError)

	var err error
	var cid, sizeVI []byte

	for {
		carUnit, chanOpen := <-anl.carDataQueue
		if !chanOpen {
			return
		}

		cid = carUnit.hdr.Cid()
		sizeVI = encoding.AppendVarint(
			sizeVI[:0],
			uint64(len(cid)+carUnit.hdr.SizeBlock()),
		)

		if _, err = anl.carDataWriter.Write(sizeVI); err == nil {
			if _, err = anl.carDataWriter.Write(cid); err == nil {
				_, err = carUnit.hdr.Content().WriteTo(anl.carDataWriter)
			}
		}

		carUnit.hdr.EvictContent()
		if carUnit.region != nil {
			carUnit.region.Release()
		}

		if err != nil {
			anl.maybeSendEvent(ErrorString, err.Error())
			anl.carWriteError <- err
			return
		}
	}
}

type splitResult struct {
	_              constants.Incomparabe
	chunkBufRegion *qringbuf.Region
	chunk          anlchunker.Chunk
}

func (anl *Anelace) processStream(streamLimit int64) error {

	// begin reading and filling buffer
	if err := anl.qrb.StartFill(streamLimit); err != nil {
		return err
	}

	var availableFromReader, processedFromReader int
	var streamOffset int64

	for {

		// next 2 lines evaluate processedInRound and availableForRound from *LAST* iteration
		streamOffset += int64(processedFromReader)
		workRegion, readErr := anl.qrb.NextRegion(availableFromReader - processedFromReader)

		if workRegion == nil || (readErr != nil && readErr != io.EOF) {
			return readErr
		}

		availableFromReader = workRegion.Size()
		processedFromReader = 0

		if err := anl.chunker.instance.Split(
			workRegion.Bytes(),
			(readErr == io.EOF),
			func(result anlchunker.Chunk) error {
				sr := splitResult{
					chunk:          result,
					chunkBufRegion: workRegion.SubRegion(processedFromReader, result.Size),
				}
				sr.chunkBufRegion.Reserve()
				anl.streamAppend(&sr)
				processedFromReader += result.Size
				anl.statSummary.Dag.Payload += int64(result.Size)

				return nil
			},
		); err != nil {
			return err
		}
	}
}

func (anl *Anelace) streamAppend(res *splitResult) {

	var ds anlblock.DataSource
	var dr *qringbuf.Region
	if res != nil {
		dr = res.chunkBufRegion
		ds.Chunk = res.chunk
		ds.Content = zcpstring.WrapSlice(dr.Bytes())
	}

	hdr := anl.collector.AppendData(ds)

	anl.curStreamOffset += int64(ds.Size)

	// The leaf block processing is entirely decoupled from the collector chain,
	// in order to not leak the Region lifetime management outside the framework
	// Collectors call that same processor on intermediate link nodes they produce
	anl.asyncWG.Add(1)
	go anl.postProcessBlock(
		hdr,
		dr,
	)
}

// This function is called as multiple "fire and forget" goroutines
// It may only try to send an error event, and it should(?) probably log.Fatal on its own
func (anl *Anelace) postProcessBlock(
	hdr *anlblock.Header,
	dataRegion *qringbuf.Region,
) {
	defer anl.asyncWG.Done()

	if constants.PerformSanityChecks {
		if hdr == nil {
			log.Panic("block registration of a nil block header reference")
		} else if hdr.SizeBlock() != 0 && hdr.SizeCumulativeDag() == 0 {
			log.Panic("block header with dag-size of 0 encountered")
		}
	}

	atomic.AddInt64(&anl.statSummary.Dag.Size, int64(hdr.SizeBlock()))
	atomic.AddInt64(&anl.statSummary.Dag.Nodes, 1)

	if hdr.SizeBlock() > 0 && anl.seenBlocks != nil {
		if k := seenKey(hdr); k != nil {

			var seen bool

			anl.mu.Lock()
			if _, seen = anl.seenBlocks[*k]; !seen {
				anl.seenBlocks[*k] = uniqueBlockStats{
					sizeBlock: hdr.SizeBlock(),
					isData:    (dataRegion != nil),
				}
			}
			anl.mu.Unlock()

			if !seen && anl.carDataQueue != nil {
				anl.carDataQueue <- carUnit{hdr: hdr, region: dataRegion}
				return // early return to avoid double-free below
			}

		}
	}

	// NOTE - these 3 steps will be done by the car emitter ( early return above )
	// if that's what the options ask for
	{
		// Ensure we are finished generating a CID before eviction
		// FIXME - there should be a mechanism to signal we will never
		// need the CID in the first place, so that no effort is spent
		hdr.Cid()

		// If we are holding parts of the qringbuf - we can drop them now
		if dataRegion != nil {
			dataRegion.Release()
		}

		// Once we processed a block in this function - dump all of its content too
		hdr.EvictContent()
	}
}
