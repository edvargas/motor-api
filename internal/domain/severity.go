package domain

// Severity is the ordered risk level a triggered rule emits.
type Severity string

const (
	SeverityBaixa   Severity = "baixa"
	SeverityMedia   Severity = "media"
	SeverityAlta    Severity = "alta"
	SeverityCritica Severity = "critica"
)

var severityRank = map[Severity]int{
	SeverityBaixa:   1,
	SeverityMedia:   2,
	SeverityAlta:    3,
	SeverityCritica: 4,
}

func (s Severity) Valid() bool {
	_, ok := severityRank[s]
	return ok
}

func (s Severity) rank() int {
	return severityRank[s]
}

// MaxSeverity returns whichever of a, b ranks higher. Unknown severities
// rank as zero, so a known severity always beats an unknown one.
func MaxSeverity(a, b Severity) Severity {
	if a.rank() >= b.rank() {
		return a
	}
	return b
}
