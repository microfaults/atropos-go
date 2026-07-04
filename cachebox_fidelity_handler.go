package atropos

import (
	"encoding/json"
	"net/http"

	"git.ucsc.edu/microfaults/atropos-go/internal/cachebox"
)

// CacheBoxFidelityHandler returns an http.Handler serving
// GET /cachebox/fidelity?experiment_id=&phase_id= (wire spec §W6) --
// manteion's synchronous, pull-based read of everything needed to compute
// a phase's VALID/INVALID verdict from this instance (design doc Q6).
//
// service and instanceID are stamped onto every snapshot; they identify
// this SDK instance, not the queried experiment/phase.
//
// Example:
//
//	mux.Handle("/cachebox/fidelity", atropos.CacheBoxFidelityHandler(cb, "cart", instanceID))
func CacheBoxFidelityHandler(cb *CacheBox, service, instanceID string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			jsonError(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		experimentID := r.URL.Query().Get("experiment_id")
		phaseID := r.URL.Query().Get("phase_id")
		if experimentID == "" || phaseID == "" {
			jsonError(w, "experiment_id and phase_id query params are required", http.StatusBadRequest)
			return
		}

		fc := cb.Fidelity().Snapshot(cachebox.PhasePair{ExperimentID: experimentID, PhaseID: phaseID})

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(FidelitySnapshot{
			InstanceID:   instanceID,
			Service:      service,
			ExperimentID: experimentID,
			PhaseID:      phaseID,
			ReplayHits:   fc.ReplayHits,
			ReplayMisses: fc.ReplayMisses,
			MissReasons: FidelityMissReasons{
				KeyAbsent:        fc.MissKeyAbsent,
				NotCommitted:     fc.MissNotCommitted,
				BodyBufferFailed: fc.MissBodyBufferFailed,
			},
			RecordEnqueued:         fc.RecordEnqueued,
			RecordPushed:           fc.RecordPushed,
			RecordDropped:          fc.RecordDropped,
			PushRejectedTerminal:   fc.PushRejectedTerminal,
			KeyCollisionsDivergent: fc.KeyCollisionsDivergent,
			KeyCollisionsIdentical: fc.KeyCollisionsIdentical,
			ReplayAgeMs: FidelityReplayAge{
				Max:  fc.ReplayAgeMaxMs,
				Mean: fc.ReplayAgeMeanMs,
			},
			Preload: FidelityPreloadState{
				Committed:   fc.PreloadCommitted,
				Entries:     fc.PreloadEntries,
				Checksum:    fc.PreloadChecksum,
				CommittedAt: fc.PreloadCommittedAt,
			},
		})
	})
}
