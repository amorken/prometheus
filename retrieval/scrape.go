package retrieval

import (
	"math/rand"
	"sync"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/log"
	"golang.org/x/net/context"

	"github.com/prometheus/prometheus/storage"
)

// Scraper retrieves samples.
type Scraper interface {
	// Sends samples to ch until an error occurrs, the context is canceled
	// or no further samples can be retrieved. The channel is closed before returning.
	Scrape(ctx context.Context, ch chan<- *model.Sample) error
}

// Scrape pools runs scrapers for a set of targets where a target, identified by
// a label set is scraped exactly once.
type ScrapePool struct {
	appender storage.SampleAppender

	ctx    context.Context
	cancel func()

	mtx    sync.RWMutex
	loops  map[model.Fingerprint]*scrapeLoop
	states map[model.Fingerprint]*TargetStatus
}

func NewScrapePool(app storage.SampleAppender) *ScrapePool {
	sp := &ScrapePool{
		appender: app,
		states:   map[model.Fingerprint]*TargetStatus{},
	}
	sp.ctx, sp.cancel = context.WithCancel(context.Background())

	return sp
}

func (sp *ScrapePool) Stop() {
	if sp.cancel != nil {
		sp.cancel()
	}
}

// Sync the running scrapers with the provided list of targets.
func (sp *ScrapePool) Sync(targets []*Target) {
	sp.mtx.Lock()
	defer sp.mtx.Unlock()

	// Shutdown all running loops after their current scrape
	// is finished.
	var wg sync.WaitGroup
	wg.Add(len(sp.loops))

	for _, l := range sp.loops {
		go func(l *scrapeLoop) {
			l.stop()
			wg.Done()
		}(l)
	}
	defer wg.Wait()

	// Old loops are shutting down, create new from the targets.
	sp.loops = map[model.Fingerprint]*scrapeLoop{}

	for _, t := range targets {
		fp := t.fingerprint()

		// Create states for new targets.
		state, ok := sp.states[fp]
		if !ok {
			state = &TargetStatus{}
			sp.states[fp] = state
		}

		// Create and start loops the first time we see a target.
		if loop, ok := sp.loops[fp]; !ok {
			loop = newScrapeLoop(sp.appender, t, state)
			go loop.run(sp.ctx, t.interval(), t.timeout())
		}
	}

	// Drop states of targets that no longer exist.
	for fp := range sp.states {
		if _, ok := sp.loops[fp]; ok {
			delete(sp.states, fp)
		}
	}
}

// scrapeLoop manages scraping cycles.
type scrapeLoop struct {
	scraper  Scraper
	state    *TargetStatus
	appender storage.SampleAppender

	cancel      func()
	stopc, done chan struct{}
	mtx         sync.RWMutex
}

func newScrapeLoop(app storage.SampleAppender, target *Target, state *TargetStatus) *scrapeLoop {
	return &scrapeLoop{
		scraper:  target,
		appender: target.wrapAppender(app),
		state:    state,
	}
}

func (sl *scrapeLoop) active() bool {
	return sl.cancel != nil
}

func (sl *scrapeLoop) run(ctx context.Context, interval, timeout time.Duration) {
	if sl.active() {
		return
	}

	sl.stopc = make(chan struct{})
	sl.done = make(chan struct{})
	defer close(sl.done)

	ctx, sl.cancel = context.WithCancel(ctx)

	select {
	case <-time.After(time.Duration(float64(interval) * rand.Float64())):
		// Continue after random scraping offset.
	case <-ctx.Done():
		return
	case <-sl.stopc:
		return
	}

	timer := time.NewTicker(interval)
	defer timer.Stop()

	for {
		start := time.Now()

		err := sl.scrape(ctx, timeout)
		if err != nil {
			log.Debugf("Scraping error: %s", err)
		}

		sl.report(start, time.Since(start), err)

		select {
		case <-sl.stopc:
			return
		case <-ctx.Done():
			return
		case <-timer.C:
		}
	}
}

// kill terminates the scraping loop immediately.
func (sl *scrapeLoop) kill() {
	if !sl.active() {
		return
	}
	sl.cancel()
}

// stop finishes any pending scrapes and then kills the loop.
func (sl *scrapeLoop) stop() {
	if !sl.active() {
		return
	}
	close(sl.stopc)
	<-sl.done

	sl.kill()
}

// scrape executes a single Scrape and appends the retrieved samples.
func (sl *scrapeLoop) scrape(ctx context.Context, timeout time.Duration) error {
	ch := make(chan *model.Sample)
	ctx, cancel := context.WithTimeout(ctx, timeout)

	defer cancel()

	var err error
	// Receive and append all the samples retrieved by the scraper.
	go func() {
		for smpl := range ch {
			// TODO(fabxc): Change the SampleAppender interface to return an error
			// so we can proceed based on the status and don't leak goroutines trying
			// to append a single sample after dropping all the other ones.
			sl.appender.Append(smpl)

			select {
			case <-ctx.Done():
				err = ctx.Err()
				return
			default:
				// Fallthrough.
			}
		}
	}()

	if err := sl.scraper.Scrape(ctx, ch); err != nil {
		return err
	}

	return err
}

func (sl *scrapeLoop) report(start time.Time, duration time.Duration, err error) {
	sl.state.setLastScrape(start)
	sl.state.setLastError(err)

	ts := model.TimeFromUnixNano(start.UnixNano())

	var health model.SampleValue
	if err == nil {
		health = 1
	}

	healthSample := &model.Sample{
		Metric: model.Metric{
			model.MetricNameLabel: scrapeHealthMetricName,
		},
		Timestamp: ts,
		Value:     health,
	}
	durationSample := &model.Sample{
		Metric: model.Metric{
			model.MetricNameLabel: scrapeDurationMetricName,
		},
		Timestamp: ts,
		Value:     model.SampleValue(float64(duration) / float64(time.Second)),
	}

	sl.appender.Append(healthSample)
	sl.appender.Append(durationSample)
}
