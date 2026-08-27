package engine

import "github.com/edvargas05/motor-deteccao/internal/domain"

// severityWeight maps each severity to its numeric weight for scoring.
// Documented formula: score = weight(max_severity)*0.7 + mean(weights)*0.3.
var severityWeight = map[domain.Severity]float64{
	domain.SeverityBaixa:   0.25,
	domain.SeverityMedia:   0.5,
	domain.SeverityAlta:    0.75,
	domain.SeverityCritica: 1.0,
}

// SeverityWeight returns the numeric weight of a severity level.
func SeverityWeight(s domain.Severity) float64 {
	return severityWeight[s]
}

// Score aggregates the severities of every triggered rule into a single
// score in [0,1]: 70% the weight of the worst (max) severity, 30% the mean
// weight across all triggered rules. This rewards a single critical hit
// while still letting several lower-severity hits push the score up.
func Score(triggered []domain.Severity) float64 {
	if len(triggered) == 0 {
		return 0
	}
	var max domain.Severity
	var sum float64
	for i, s := range triggered {
		if i == 0 {
			max = s
		} else {
			max = domain.MaxSeverity(max, s)
		}
		sum += SeverityWeight(s)
	}
	mean := sum / float64(len(triggered))
	return SeverityWeight(max)*0.7 + mean*0.3
}
