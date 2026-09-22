package outbox

import "testing"

func TestTable_ValidateRejectsAnythingThatIsNotAPlainIdentifier(t *testing.T) {
	cases := []struct {
		name  string
		table Table
		ok    bool
	}{
		{"orders", Table{"outbox", "order_id"}, true},
		{"underscore start", Table{"_outbox", "k"}, true},
		{"empty name", Table{"", "order_id"}, false},
		{"empty key", Table{"outbox", ""}, false},
		{"injection in the table", Table{"outbox; DROP TABLE orders; --", "order_id"}, false},
		{"injection in the key", Table{"outbox", "order_id, payload"}, false},
		{"quoted", Table{`"outbox"`, "order_id"}, false},
		{"schema qualified", Table{"public.outbox", "order_id"}, false},
		{"upper case", Table{"Outbox", "order_id"}, false},
		{"starts with digit", Table{"1outbox", "order_id"}, false},
		{"too long", Table{"a123456789012345678901234567890123456789012345678901234567890123", "k"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.table.validate(); (err == nil) != tc.ok {
				t.Errorf("validate(%+v) = %v, want ok=%v", tc.table, err, tc.ok)
			}
		})
	}
}

func TestNewPublisher_PanicsOnABadTableInsteadOfBuildingBadSQL(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("NewPublisher accepted an injectable table name")
		}
	}()
	NewPublisher(nil, nil, Table{Name: "outbox; DROP TABLE x", KeyColumn: "k"})
}
