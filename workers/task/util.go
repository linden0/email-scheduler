package task

func checkWorkerUsed(workers []string, id string) bool {
	for _, worker := range workers {
		if worker == id {
			return true
		}
	}
	return false
}

func removeWorker(workers []string, id string) []string {
	var newWorkers []string
	for _, worker := range workers {
		if worker != id {
			newWorkers = append(newWorkers, worker)
		}
	}
	return newWorkers
}