// Package moex contains functions to work with Moscow Exchange api
package moex

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"securitiesModule/securities"
	"sort"
	"strings"
	"sync"
	"time"
)

// moexCandle is a type to parse Moscow Exchange json
type moexCandle struct {
	CandleData [][]any `json:"data"`
}

// moexCandles is a type to parse Moscow Exchange json
type moexCandles struct {
	Candles moexCandle `json:"candles"`
}

// moexHistoryRecord is a type to parse Moscow Exchange json
type moexHistoryRecord struct {
	HistoryRecordData [][]any `json:"data"`
}

// moexHistory is a type to parse Moscow Exchange json
type moexHistory struct {
	History moexHistoryRecord `json:"history"`
}

// getEngineAndMarket returns engine and market of security type to use in Moscow Exchange api request
func getEngineAndMarket(sType securities.SecurityType) (engine string, market string, board string, err error) {
	switch sType {
	case securities.Share:
		engine = "stock"
		market = "shares"
		board = ""
	case securities.ETF:
		engine = "stock"
		market = "shares"
		board = "TQTF"
	case securities.Bond:
		engine = "stock"
		market = "bonds"
		board = ""
	case securities.Currency:
		engine = "currency"
		market = "index"
		board = ""
	default:
		err = fmt.Errorf("unknown security type: %s", sType)
	}

	return
}

// GetSecurityQuotes gets quotes of the given security of the given interval for the given period from Moscow Exchange
func GetSecurityQuotes(sec *securities.Security, dateFrom time.Time, dateTill time.Time, interval securities.QuotesInterval) error {
	engine, market, board, err := getEngineAndMarket(sec.SType())
	if err != nil {
		return err
	}

	boardStr := ""
	if board != "" {
		boardStr = "/boards/" + board
	}

	request := fmt.Sprintf("https://iss.moex.com/iss/engines/%s/markets/%s%s/securities/%s/candles.json?from=%s&till=%s&interval=%s",
		engine, market, boardStr, sec.Id(), dateFrom.Format("2006-01-02"), dateTill.Format("2006-01-02"), fmt.Sprint(interval))

	resp, err := http.Get(request)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	moexCandles := moexCandles{}
	err = json.Unmarshal(body, &moexCandles)
	if err != nil {
		return err
	}

	wg := new(sync.WaitGroup)
	mu := new(sync.Mutex)

	var quotes []securities.SecurityQuotes
	for _, candle := range moexCandles.Candles.CandleData {
		wg.Add(1)

		go func(candle []any) {
			defer wg.Done()

			begin, err := time.Parse("2006-01-02 15:04:05", candle[6].(string))
			if err != nil {
				log.Fatal("can't convert Moscow Exchange date format: " + candle[6].(string))
			}

			end, err := time.Parse("2006-01-02 15:04:05", candle[7].(string))
			if err != nil {
				log.Fatal("can't convert Moscow Exchange date format: " + candle[6].(string))
			}

			secQuotes := securities.SecurityQuotes{
				Interval: interval,
				Begin:    begin,
				End:      end,
				Open:     candle[0].(float64),
				Close:    candle[1].(float64),
				High:     candle[2].(float64),
				Low:      candle[3].(float64),
			}

			mu.Lock()
			quotes = append(quotes, secQuotes)
			mu.Unlock()
		}(candle)
	}

	wg.Wait()

	sort.Slice(quotes, func(i, j int) bool {
		return quotes[j].Begin.After(quotes[i].Begin)
	})

	sec.SetQuotesList(&quotes)

	return nil
}

// GetQuotesForDate gets quotes for the given list of securities on the given date from Moscow Exchange
func GetQuotesForDate(sec []*securities.Security, date time.Time) error {
	// No concurrency for Moscow Exchange requests - we may be blocked for this
	wg := new(sync.WaitGroup)

	sTypes := make(map[securities.SecurityType]bool)
	sIds := make(map[string]*securities.Security)
	for _, s := range sec {
		sTypes[s.SType()] = true
		sIds[s.Id()] = s
	}

	for sType := range sTypes {
		engine, market, board, err := getEngineAndMarket(sType)
		if err != nil {
			return err
		}

		boardStr := ""
		if board != "" {
			boardStr = "/boards/" + board
		}

		for start := 0; start < 1000; start += 100 {
			request := fmt.Sprintf("https://iss.moex.com/iss/history/engines/%s/markets/%s%s/securities.json?date=%s&start=%s",
				engine, market, boardStr, date.Format("2006-01-02"), fmt.Sprint(start))

			resp, err := http.Get(request)
			if err != nil {
				return err
			}
			defer resp.Body.Close()

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				return err
			}

			moexHistory := moexHistory{}
			err = json.Unmarshal(body, &moexHistory)
			if err != nil {
				return err
			}

			if len(moexHistory.History.HistoryRecordData) == 0 {
				if start == 0 {
					// no data for this day - let's look on previous day
					return GetQuotesForDate(sec, date.AddDate(0, 0, -1))
				}

				break
			}

			boardToCheck := "TQBR"
			if board != "" {
				boardToCheck = board
			}

			for _, data := range moexHistory.History.HistoryRecordData {
				wg.Add(1)

				go func(data []any) {
					defer wg.Done()

					if data[0].(string) == boardToCheck && data[3] != nil && data[11] != nil {
						s, ok := sIds[strings.ToUpper(data[3].(string))]
						if !ok {
							return
						}

						s.SetQuotes(securities.SecurityQuotes{
							Interval: securities.IntervalDay,
							Begin:    date.Truncate(24 * time.Hour),
							End:      date.AddDate(0, 0, 1).Truncate(24 * time.Hour),
							Open:     data[6].(float64),
							Close:    data[11].(float64),
							High:     data[8].(float64),
							Low:      data[7].(float64),
						})
					}
				}(data)
			}

			wg.Wait()
		}
	}

	return nil
}
