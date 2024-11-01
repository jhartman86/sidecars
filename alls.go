package sidecars

import (
	"pkg/sys"
	"pkg/sys/logging"
	"sync"
)

func Run(onlyHandles ...string) {
	byHandles := map[string]struct{}{}
	for _, h := range onlyHandles {
		byHandles[h] = struct{}{}
	}

	handles := []string{}
	for h := range manager.cars {
		if len(onlyHandles) >= 1 {
			if _, ok := byHandles[h]; !ok {
				continue
			}
		}
		manager.cars[h].exec()
		handles = append(handles, manager.cars[h].Handle)
	}
	logging.GetLogger().Infof(`-- [SIDECARS] Running (%s) --`, handles)

	// graceful shutdown
	sys.OnSigTerm(func() error {
		DrainAll()
		return nil
	})
	// manager.Wait() // @todo: necessary
}

func DrainAll() {
	manager.Lock()
	if len(manager.cars) == 0 {
		manager.Unlock()
		return
	}
	manager.Unlock()

	var awaitLogFlush sync.WaitGroup
	manager.drainAll.Do(func() {
		for h := range manager.cars {
			awaitLogFlush.Add(1)
			go func(sc *Sidecar) {
				defer awaitLogFlush.Done()
				sc.drain() // blocks
			}(manager.cars[h])
		}
	})

	manager.Wait()
	awaitLogFlush.Wait()
}
