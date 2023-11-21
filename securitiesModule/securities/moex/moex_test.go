package moex

import (
	"securitiesModule/securities"
	"testing"
	"time"
)

func TestGetSecurityQuotes(t *testing.T) {
	secGAZP := securities.GetQuickSecurity("GAZP", securities.Share)

	err := GetSecurityQuotes(secGAZP, time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2022, 1, 31, 0, 0, 0, 0, time.UTC), securities.IntervalDay)
	if err != nil {
		t.Fatal(err)
	}

	date := time.Date(2022, 1, 16, 0, 0, 0, 0, time.Now().Location())
	priceForDate := secGAZP.QuotesForDate(securities.IntervalDay, date)
	lastPrice := secGAZP.LastQuotes(securities.IntervalDay)

	if priceForDate.Close != 335.76 {
		t.Errorf("wrong price for date (GAZP on 16.01.2022) - want 335.76, got %f", priceForDate.Close)
	}

	if lastPrice.Close != 334.8 {
		t.Errorf("wrong last price (GAZP on 31.01.2022) - want 334.8, got %f", lastPrice.Close)
	}
}

func TestGetQuotesForDate(t *testing.T) {
	secGAZP := securities.GetQuickSecurity("GAZP", securities.Share)
	secLKOH := securities.GetQuickSecurity("LKOH", securities.Share)

	securitiesList := []*securities.Security{secGAZP, secLKOH}
	err := GetQuotesForDate(securitiesList, time.Date(2022, 2, 4, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}

	lastPr1 := secGAZP.LastQuotes(securities.IntervalDay)
	if lastPr1.Close != 324.6 {
		t.Errorf("wrong last price (GAZP on 4.02.2022) - want 324.6, got %f", lastPr1.Close)
	}

	lastPr2 := secLKOH.LastQuotes(securities.IntervalDay)
	if lastPr2.Close != 7010.0 {
		t.Errorf("wrong last price (LKOH on 4.02.2022) - want 7010, got %f", lastPr2.Close)
	}
}
