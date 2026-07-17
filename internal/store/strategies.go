package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// HourWindow / Trigger / Leg / Event mirror ge-agent's kind-specific
// structured fields (local copy: the wire contract is the sidecar file, not
// a shared Go module — the repos deploy independently).
type HourWindow struct {
	FromHow int `json:"from_how"` // 0-167 = dow*24+hour UTC, dow 0=Sunday
	ToHow   int `json:"to_how"`   // inclusive; from > to wraps the week
}

// Contains reports whether hour-of-week bucket b falls inside the window.
func (w HourWindow) Contains(b int) bool {
	if w.FromHow <= w.ToHow {
		return b >= w.FromHow && b <= w.ToHow
	}
	return b >= w.FromHow || b <= w.ToHow
}

type Trigger struct {
	Metric    string  `json:"metric"` // volume_zscore | price_move_pct
	Threshold float64 `json:"threshold"`
	Direction string  `json:"direction"` // above | below
	Window    string  `json:"window"`
}

type Leg struct {
	ItemID int    `json:"item_id"`
	Name   string `json:"name"`
	Side   string `json:"side"` // buy | sell
	Qty    int64  `json:"qty"`
	Price  int64  `json:"price"`
}

type Event struct {
	Date        string `json:"date"` // YYYY-MM-DD UTC
	Description string `json:"description"`
}

type SignalVerdict struct {
	SignalID int    `json:"signal_id"`
	Verdict  string `json:"verdict"` // shipped | dismissed
	Reason   string `json:"reason"`
}

// SidecarStrategy mirrors ge-agent's internal/strategy.Strategy JSON shape.
type SidecarStrategy struct {
	ID        string `json:"id"`
	Archetype string `json:"archetype"`
	Title     string `json:"title"`
	Thesis    string `json:"thesis"`
	Items     []struct {
		Name     string `json:"name"`
		ID       int    `json:"id"`
		BuyLimit *int64 `json:"buy_limit"`
		Members  *bool  `json:"members"`
	} `json:"items"`
	Entry           string `json:"entry"`
	Exit            string `json:"exit"`
	EntryPrice      int64  `json:"entry_price"`
	ExitPrice       int64  `json:"exit_price"`
	KillPrice       *int64 `json:"kill_price"`
	Horizon         string `json:"horizon"`
	CapitalRequired int64  `json:"capital_required"`
	Size            struct {
		BuyLimit       int64 `json:"buy_limit"`
		VolConstrained int64 `json:"vol_constrained"`
		UnitsUsed      int64 `json:"units_used"`
	} `json:"size"`
	ExpectedValue struct {
		PerCycleGp int64   `json:"per_cycle_gp"`
		Per1hGp    int64   `json:"per_1h_gp"`
		PerDayGp   int64   `json:"per_day_gp"`
		RoiPct     float64 `json:"roi_pct"`
	} `json:"expected_value"`
	Confidence    string   `json:"confidence"`
	ConfidenceWhy string   `json:"confidence_why"`
	Evidence      string   `json:"evidence"`
	Invalidation  string   `json:"invalidation"`
	Risks         []string `json:"risks"`
	PaperTrade    string   `json:"paper_trade"`

	BuyWindow       *HourWindow `json:"buy_window,omitempty"`
	SellWindow      *HourWindow `json:"sell_window,omitempty"`
	Trigger         *Trigger    `json:"trigger,omitempty"`
	Direction       *string     `json:"direction,omitempty"`
	Legs            []Leg       `json:"legs,omitempty"`
	RelationID      *int        `json:"relation_id,omitempty"`
	Event           *Event      `json:"event,omitempty"`
	EvalWindowHours *int        `json:"eval_window_hours,omitempty"`
}

type Sidecar struct {
	RunStartedAt   time.Time         `json:"run_started_at"`
	ReportPath     string            `json:"report_path"`
	Strategies     []SidecarStrategy `json:"strategies"`
	SignalVerdicts []SignalVerdict   `json:"signal_verdicts,omitempty"`
}

// evalWindowHours resolves the paper-trading window: the model's explicit
// choice, else the per-kind default (S one weekly cycle; V from trigger fire;
// U around the event; C/legacy 48h).
func evalWindowHours(st SidecarStrategy) int {
	if st.EvalWindowHours != nil {
		return *st.EvalWindowHours
	}
	switch st.Archetype {
	case "S":
		return 168
	case "V":
		return 96
	case "U":
		return 72
	default:
		return 48
	}
}

// PrimaryItemID picks the item the scalar snapshot columns and kill_price key
// on. For C that's the first sell leg (the output you mark the conversion
// to); everything else keys on items[0].
func (st SidecarStrategy) PrimaryItemID() int {
	if st.Archetype == "C" {
		for _, l := range st.Legs {
			if l.Side == "sell" {
				return l.ItemID
			}
		}
	}
	return st.Items[0].ID
}

// Vetoed pairs a sidecar strategy with the ship-time rule that rejected it.
type Vetoed struct {
	Strategy SidecarStrategy
	Reason   string
}

// InsertStrategies ingests a run's sidecar strategies in one transaction.
// V strategies enter ARMED — the evaluator opens them when their trigger
// fires; everything else opens immediately. Vetoed strategies are stored
// closed with their rejection reason so nothing disappears silently.
func (s *Store) InsertStrategies(ctx context.Context, runID int64, openedAt time.Time, list []SidecarStrategy, vetoed []Vetoed) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	for _, st := range list {
		state := "open"
		if st.Archetype == "V" {
			state = "armed"
		}
		if err := insertStrategy(ctx, tx, runID, openedAt, st, state, nil, nil); err != nil {
			return err
		}
	}
	for _, v := range vetoed {
		if err := insertStrategy(ctx, tx, runID, openedAt, v.Strategy, "vetoed", &v.Reason, &openedAt); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func insertStrategy(ctx context.Context, tx pgx.Tx, runID int64, openedAt time.Time, st SidecarStrategy, state string, reason *string, closedAt *time.Time) error {
	if len(st.Items) == 0 {
		return fmt.Errorf("strategy %s: no items", st.ID)
	}
	items, _ := json.Marshal(st.Items)
	risks, _ := json.Marshal(st.Risks)
	marshalOrNil := func(v any, present bool) any {
		if !present {
			return nil
		}
		b, _ := json.Marshal(v)
		return b
	}
	if _, err := tx.Exec(ctx, `INSERT INTO orchestrator.strategies
		(run_id, sid, archetype, title, thesis, items, primary_item_id,
		 entry_text, exit_text, entry_price, exit_price, kill_price, horizon_text,
		 eval_window, capital_required, units_used,
		 per_cycle_gp, per_1h_gp, per_day_gp, roi_pct,
		 confidence, confidence_why, evidence, invalidation, risks, paper_trade,
		 state, state_reason, opened_at, closed_at,
		 buy_window, sell_window, trigger, direction, legs, relation_id, event)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,
		        make_interval(hours => $14),$15,$16,$17,$18,$19,$20,
		        $21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37)`,
		runID, st.ID, st.Archetype, st.Title, st.Thesis, items, st.PrimaryItemID(),
		st.Entry, st.Exit, st.EntryPrice, st.ExitPrice, st.KillPrice, st.Horizon,
		evalWindowHours(st), st.CapitalRequired, st.Size.UnitsUsed,
		st.ExpectedValue.PerCycleGp, st.ExpectedValue.Per1hGp, st.ExpectedValue.PerDayGp, st.ExpectedValue.RoiPct,
		st.Confidence, st.ConfidenceWhy, st.Evidence, st.Invalidation, risks, st.PaperTrade,
		state, reason, openedAt, closedAt,
		marshalOrNil(st.BuyWindow, st.BuyWindow != nil),
		marshalOrNil(st.SellWindow, st.SellWindow != nil),
		marshalOrNil(st.Trigger, st.Trigger != nil),
		st.Direction,
		marshalOrNil(st.Legs, len(st.Legs) > 0),
		st.RelationID,
		marshalOrNil(st.Event, st.Event != nil),
	); err != nil {
		return fmt.Errorf("insert strategy %s: %w", st.ID, err)
	}
	return nil
}

// CommittedCapital sums capital_required across the live book (open + armed) —
// the number ship-time capital vetting charges new strategies against.
func (s *Store) CommittedCapital(ctx context.Context) (int64, error) {
	var total int64
	err := s.Pool.QueryRow(ctx, `SELECT coalesce(sum(capital_required), 0)
		FROM orchestrator.strategies WHERE state IN ('open','armed')`).Scan(&total)
	return total, err
}

// HasOpenStrategyForItem reports whether the live book already trades this
// item under this archetype (the ship-time dedup rule).
func (s *Store) HasOpenStrategyForItem(ctx context.Context, itemID int, archetype string) (bool, error) {
	var exists bool
	err := s.Pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM orchestrator.strategies
		WHERE state IN ('open','armed') AND primary_item_id=$1 AND archetype=$2)`,
		itemID, archetype).Scan(&exists)
	return exists, err
}

type Strategy struct {
	StrategyID    int64           `json:"strategy_id"`
	RunID         int64           `json:"run_id"`
	Sid           string          `json:"sid"`
	Archetype     string          `json:"archetype"`
	Title         string          `json:"title"`
	Thesis        string          `json:"thesis"`
	Items         json.RawMessage `json:"items"`
	PrimaryItemID int             `json:"primary_item_id"`
	EntryText     string          `json:"entry"`
	ExitText      string          `json:"exit"`
	EntryPrice    int64           `json:"entry_price"`
	ExitPrice     int64           `json:"exit_price"`
	KillPrice     *int64          `json:"kill_price"`
	HorizonText   string          `json:"horizon"`
	Capital       *int64          `json:"capital_required"`
	UnitsUsed     *int64          `json:"units_used"`
	PerCycleGp    *int64          `json:"per_cycle_gp"`
	Per1hGp       *int64          `json:"per_1h_gp"`
	PerDayGp      *int64          `json:"per_day_gp"`
	RoiPct        *float64        `json:"roi_pct"`
	Confidence    string          `json:"confidence"`
	ConfidenceWhy *string         `json:"confidence_why"`
	Invalidation  string          `json:"invalidation"`
	Risks         json.RawMessage `json:"risks"`
	PaperTrade    *string         `json:"paper_trade"`
	State         string          `json:"state"`
	StateReason   *string         `json:"state_reason"`
	OpenedAt      time.Time       `json:"opened_at"`
	ClosedAt      *time.Time      `json:"closed_at"`

	EvalWindow  time.Duration `json:"-"`
	EvalWindowS float64       `json:"eval_window_s"`
	BuyWindow   *HourWindow   `json:"buy_window,omitempty"`
	SellWindow  *HourWindow   `json:"sell_window,omitempty"`
	Trigger     *Trigger      `json:"trigger,omitempty"`
	Direction   *string       `json:"direction,omitempty"`
	Legs        []Leg         `json:"legs,omitempty"`
	RelationID  *int          `json:"relation_id,omitempty"`
	Event       *Event        `json:"event,omitempty"`
	TriggeredAt *time.Time    `json:"triggered_at,omitempty"`
}

const strategyCols = `strategy_id, run_id, sid, archetype, title, thesis, items, primary_item_id,
	entry_text, exit_text, entry_price, exit_price, kill_price, horizon_text,
	capital_required, units_used, per_cycle_gp, per_1h_gp, per_day_gp, roi_pct,
	confidence, confidence_why, invalidation, risks, paper_trade,
	state, state_reason, opened_at, closed_at,
	extract(epoch from eval_window)::float8,
	buy_window, sell_window, trigger, direction, legs, relation_id, event, triggered_at`

func scanStrategy(row pgx.Row) (*Strategy, error) {
	var st Strategy
	var buyW, sellW, trig, legs, event []byte
	err := row.Scan(&st.StrategyID, &st.RunID, &st.Sid, &st.Archetype, &st.Title, &st.Thesis,
		&st.Items, &st.PrimaryItemID, &st.EntryText, &st.ExitText, &st.EntryPrice, &st.ExitPrice,
		&st.KillPrice, &st.HorizonText, &st.Capital, &st.UnitsUsed,
		&st.PerCycleGp, &st.Per1hGp, &st.PerDayGp, &st.RoiPct,
		&st.Confidence, &st.ConfidenceWhy, &st.Invalidation, &st.Risks, &st.PaperTrade,
		&st.State, &st.StateReason, &st.OpenedAt, &st.ClosedAt,
		&st.EvalWindowS, &buyW, &sellW, &trig, &st.Direction, &legs, &st.RelationID, &event, &st.TriggeredAt)
	if err != nil {
		return nil, err
	}
	st.EvalWindow = time.Duration(st.EvalWindowS * float64(time.Second))
	for raw, target := range map[*[]byte]any{
		&buyW: &st.BuyWindow, &sellW: &st.SellWindow, &trig: &st.Trigger,
		&legs: &st.Legs, &event: &st.Event,
	} {
		if len(*raw) > 0 {
			if err := json.Unmarshal(*raw, target); err != nil {
				return nil, fmt.Errorf("strategy %d: decode structured field: %w", st.StrategyID, err)
			}
		}
	}
	return &st, nil
}

func (s *Store) collectStrategies(ctx context.Context, query string, args ...any) ([]Strategy, error) {
	rows, err := s.Pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Strategy
	for rows.Next() {
		st, err := scanStrategy(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *st)
	}
	return out, rows.Err()
}

func (s *Store) StrategiesForRun(ctx context.Context, runID int64) ([]Strategy, error) {
	return s.collectStrategies(ctx,
		`SELECT `+strategyCols+` FROM orchestrator.strategies WHERE run_id=$1 ORDER BY strategy_id`, runID)
}

// LatestRunStrategies returns the strategies of the most recent succeeded run.
func (s *Store) LatestRunStrategies(ctx context.Context) ([]Strategy, error) {
	return s.collectStrategies(ctx, `SELECT `+strategyCols+` FROM orchestrator.strategies
		WHERE run_id = (SELECT max(run_id) FROM orchestrator.runs WHERE status='succeeded')
		ORDER BY per_1h_gp DESC NULLS LAST`)
}

// EvaluableStrategies returns everything the ticker must look at: open
// strategies (price checks) and armed ones (trigger checks).
func (s *Store) EvaluableStrategies(ctx context.Context) ([]Strategy, error) {
	return s.collectStrategies(ctx,
		`SELECT `+strategyCols+` FROM orchestrator.strategies WHERE state IN ('open','armed') ORDER BY strategy_id`)
}

// MarkTriggered flips an armed V strategy open, anchoring its evaluation
// clock at the trigger moment.
func (s *Store) MarkTriggered(ctx context.Context, id int64, at time.Time) error {
	_, err := s.Pool.Exec(ctx, `UPDATE orchestrator.strategies
		SET state='open', triggered_at=$2 WHERE strategy_id=$1 AND state='armed'`, id, at)
	return err
}

func (s *Store) StrategyByID(ctx context.Context, id int64) (*Strategy, error) {
	st, err := scanStrategy(s.Pool.QueryRow(ctx,
		`SELECT `+strategyCols+` FROM orchestrator.strategies WHERE strategy_id=$1`, id))
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	return st, err
}

func (s *Store) CloseStrategy(ctx context.Context, id int64, state, reason string) error {
	_, err := s.Pool.Exec(ctx, `UPDATE orchestrator.strategies
		SET state=$2, state_reason=$3, closed_at=now() WHERE strategy_id=$1 AND state IN ('open','armed')`,
		id, state, reason)
	return err
}
