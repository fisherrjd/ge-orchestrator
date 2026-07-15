package eval

import (
	"context"

	"github.com/osrs-ge/ge-orchestrator/internal/store"
)

// computeCombo evaluates a C strategy: re-price every leg at the latest
// quotes (one round trip), compose the post-tax conversion margin, and judge
// it against the projected one. The WORST leg governs freshness and volume —
// a conversion is only as tradeable as its least liquid side.
func (ev *Evaluator) computeCombo(ctx context.Context, st store.Strategy) (store.Evaluation, map[string]bool, error) {
	now := ev.now()
	ids := make([]int, 0, len(st.Legs))
	for _, l := range st.Legs {
		ids = append(ids, l.ItemID)
	}
	snaps, err := ev.source().SnapshotMany(ctx, ids)
	if err != nil {
		return store.Evaluation{}, nil, err
	}

	checks := map[string]bool{}
	allFresh, allPriced := true, true
	volOK := true
	var marginNowRaw, marginNowHaircut int64
	var minLegConversions *int64 // conversions/4h the thinnest leg supports at participation
	legDetails := make([]map[string]any, 0, len(st.Legs))

	units := int64(0)
	if st.UnitsUsed != nil {
		units = *st.UnitsUsed
	}

	for _, l := range st.Legs {
		snap := snaps[l.ItemID]
		d := map[string]any{"item_id": l.ItemID, "name": l.Name, "side": l.Side, "qty": l.Qty}
		if snap == nil {
			allPriced, allFresh = false, false
			legDetails = append(legDetails, d)
			continue
		}
		var price *int64
		var ageS *int
		if l.Side == "buy" {
			price, ageS = snap.Low, snap.LowAgeS
		} else {
			price, ageS = snap.High, snap.HighAgeS
		}
		d["price"], d["age_s"], d["vol_30m"] = price, ageS, snap.Vol30m
		legDetails = append(legDetails, d)

		if price == nil {
			allPriced = false
			continue
		}
		if ageS == nil || *ageS > freshMaxAgeS {
			allFresh = false
		}
		if l.Side == "buy" {
			marginNowRaw -= *price * l.Qty
			marginNowHaircut -= ev.slipBuy(*price) * l.Qty
		} else {
			marginNowRaw += (*price - sellTax(*price)) * l.Qty
			sp := ev.slipSell(*price)
			marginNowHaircut += (sp - sellTax(sp)) * l.Qty
		}
		// This leg supports vol_30m*8/qty conversions per 4h at full volume;
		// participation caps it further.
		legConv := int64(ev.participation() * float64(snap.Vol30m*8) / float64(l.Qty))
		if minLegConversions == nil || legConv < *minLegConversions {
			minLegConversions = &legConv
		}
		if units > 0 && snap.Vol30m < units*l.Qty/volDivisor {
			volOK = false
		}
	}

	checks["legs_priced"] = allPriced
	checks["legs_fresh"] = allFresh
	checks["vol_ok"] = volOK

	projected := st.ExitPrice - st.EntryPrice // per conversion, post-tax by contract
	marginOK := allPriced && float64(marginNowRaw) >= marginOKFraction*float64(projected)
	checks["margin_ok"] = marginOK

	var realizedPer1h, rawPer1h *int64
	if allPriced {
		capped := units
		if minLegConversions != nil && *minLegConversions < capped {
			capped = *minLegConversions
		}
		raw := marginNowRaw * units / 4
		hc := marginNowHaircut * capped / 4
		rawPer1h, realizedPer1h = &raw, &hc
	}

	verdict := "healthy"
	switch {
	case allPriced && !marginOK:
		verdict = "kill_signal"
	case !allPriced || !allFresh || !volOK:
		verdict = "degraded"
	}

	// Scalar snapshot columns carry the primary (first sell) leg.
	e := baseEvaluation(st, now, snaps[st.PrimaryItemID])
	e.RealizedPer1hGp = realizedPer1h
	finishEvaluation(&e, checks, verdict, map[string]any{
		"legs": legDetails,
		"combo_margin_now": marginNowRaw, "combo_margin_haircut": marginNowHaircut,
		"projected_margin": projected,
		"realized_raw_per_1h": rawPer1h, "realized_haircut_per_1h": realizedPer1h,
	})
	return e, checks, nil
}
