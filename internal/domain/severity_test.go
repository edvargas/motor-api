package domain

import "testing"

func TestMaxSeverity(t *testing.T) {
	cases := []struct {
		a, b, want Severity
	}{
		{SeverityBaixa, SeverityAlta, SeverityAlta},
		{SeverityCritica, SeverityMedia, SeverityCritica},
		{SeverityMedia, SeverityMedia, SeverityMedia},
	}
	for _, c := range cases {
		got := MaxSeverity(c.a, c.b)
		if got != c.want {
			t.Errorf("MaxSeverity(%q, %q) = %q, want %q", c.a, c.b, got, c.want)
		}
	}
}
