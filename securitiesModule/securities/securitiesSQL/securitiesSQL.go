// Package securitiesSQL contains functions to work with securities data in SQL database
package securitiesSQL

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"securitiesModule/securities"
	"securitiesModule/securities/moex"
	"sort"
	"strings"
	"sync"
	"time"
)

// collectErrors collects errors from error channel and send the result into final error channel
// Not the best place for this function and not the best way to deal with errors but let it be so for now
func collectErrors(quitChan chan bool, finErrChan chan error, errChan chan error) {
	var err, finErr error

	for {
		select {
		case err = <-errChan:
			if finErr == nil {
				finErr = err
			} else {
				finErr = errors.New(finErr.Error() + "\n" + err.Error())
			}
		case <-quitChan:
			{
				close(errChan)
				close(quitChan)
				finErrChan <- finErr
				return
			}
		}
	}
}

// SecurityExists checks if security with given id and type exists in database
func SecurityExists(db *sql.DB, id string, sType securities.SecurityType) (bool, error) {
	if id == "" {
		return false, errors.New("security has no id")
	}

	if sType == "" || sType == securities.UnknownType {
		return false, errors.New("security has no type or type is unknown")
	}

	// checking security type in query is safe
	// checking security id is not
	queryText := fmt.Sprintf("SELECT id FROM securities WHERE type = \"%s\"", sType)
	resDB, err := db.Query(queryText)
	if err != nil {
		return false, err
	}

	var resDBRow struct {
		id string
	}
	for resDB.Next() {
		err = resDB.Scan(&resDBRow.id)
		if err != nil {
			return false, err
		}

		if resDBRow.id == id {
			return true, nil
		}
	}

	return false, nil
}

// SecurityQuotesExist checks if security quotes for the given begin date and the given interval exist in database
func SecurityQuotesExist(db *sql.DB, sec *securities.Security, date time.Time, interval securities.QuotesInterval) (bool, error) {
	queryText := fmt.Sprintf("SELECT * FROM security_quotes WHERE security = \"%s\" AND begin = \"%s\" AND interv = \"%d\"",
		sec.Id(), date.UTC().Format("2006-01-02 15:04:05"), interval)

	res, err := db.Query(queryText)
	if err != nil {
		return false, err
	}
	defer res.Close()

	if res.Next() {
		return true, nil
	}

	return false, nil
}

// GetSecurityData fills in security data from database
func GetSecurityData(db *sql.DB, sec *securities.Security) error {
	seqExists, err := SecurityExists(db, sec.Id(), sec.SType())
	if err != nil {
		return err
	}

	if !seqExists {
		return fmt.Errorf("security %s does not exist", sec.Id())
	}

	sQueryText := fmt.Sprintf("SELECT name, currency FROM securities WHERE id = \"%s\"", sec.Id()) // safe!
	sResDB, err := db.Query(sQueryText)
	if err != nil {
		return err
	}

	var sResDBRow struct {
		name     string
		currency string
	}

	_ = sResDB.Next() // we know that security exists
	err = sResDB.Scan(&sResDBRow.name, &sResDBRow.currency)
	if err != nil {
		return err
	}

	sec.SetName(sResDBRow.name)
	sec.SetCurrency(securities.GetSecurityCurrencyFromString(sResDBRow.currency))

	sqQueryText := fmt.Sprintf("SELECT interv, begin, end, open, close, high, low FROM security_quotes WHERE security = \"%s\"", sec.Id()) // safe!
	sqResDB, err := db.Query(sqQueryText)
	if err != nil {
		return err
	}

	type sqResDBRow struct {
		interval int
		begin    []uint8
		end      []uint8
		open     float64
		close    float64
		high     float64
		low      float64
	}

	wg := new(sync.WaitGroup)
	mu := new(sync.Mutex)

	for sqResDB.Next() {
		var sqResDBRowOne sqResDBRow

		err = sqResDB.Scan(&sqResDBRowOne.interval, &sqResDBRowOne.begin, &sqResDBRowOne.end, &sqResDBRowOne.open, &sqResDBRowOne.close, &sqResDBRowOne.high, &sqResDBRowOne.low)
		if err != nil {
			return err
		}

		wg.Add(1)

		go func(sqResDBRowOne sqResDBRow) {
			defer wg.Done()

			strBeginDate := string(sqResDBRowOne.begin)
			strEndDate := string(sqResDBRowOne.end)
			if strBeginDate != "" && strEndDate != "" {
				beginDate, err := time.Parse("2006-01-02 15:04:05", strBeginDate)
				if err != nil {
					log.Fatal("can't convert database date format: " + strBeginDate)
				}

				endDate, err := time.Parse("2006-01-02 15:04:05", strEndDate)
				if err != nil {
					log.Fatal("can't convert database date format: " + strEndDate)
				}

				sQuotes := securities.SecurityQuotes{
					Interval: securities.QuotesInterval(sqResDBRowOne.interval),
					Begin:    beginDate,
					End:      endDate,
					Open:     sqResDBRowOne.open,
					Close:    sqResDBRowOne.close,
					High:     sqResDBRowOne.high,
					Low:      sqResDBRowOne.low,
				}

				mu.Lock()
				sec.SetQuotes(sQuotes)
				mu.Unlock()
			}
		}(sqResDBRowOne)
	}

	wg.Wait()

	q := sec.Quotes()

	sort.Slice(*q, func(i, j int) bool {
		return (*q)[j].Begin.After((*q)[i].Begin)
	})

	sec.ClearAndSetQuotesList(q)

	return nil
}

// GetSecuritiesData fills in data for a list of securities from database
func GetSecuritiesData(db *sql.DB, sec []*securities.Security) error {
	wg := new(sync.WaitGroup)
	quitChan := make(chan bool)
	finErrChan := make(chan error)
	errChan := make(chan error)

	go collectErrors(quitChan, finErrChan, errChan)

	for _, s := range sec {
		wg.Add(1)

		go func(s *securities.Security, errChan chan error) {
			defer wg.Done()

			err := GetSecurityData(db, s)

			if err != nil {
				errChan <- err
			}
		}(s, errChan)
	}

	wg.Wait()

	quitChan <- true

	err := <-finErrChan
	close(finErrChan)
	if err != nil {
		return err
	}

	return nil
}

// GetAllSecuritiesData fills in data for all existing in database securities (considering type and currency filters) with only last quotes for each security
func GetAllSecuritiesData(db *sql.DB, typeNameFilter string, currencyNameFilter string) ([]*securities.Security, error) {
	if typeNameFilter != "" {
		sType := securities.GetSecurityTypeFromString(typeNameFilter)
		if sType == securities.UnknownType {
			return nil, fmt.Errorf("wrong type name: %s", typeNameFilter)
		}
	}

	if currencyNameFilter != "" {
		currency := securities.GetSecurityCurrencyFromString(currencyNameFilter)
		if currency == securities.UnknownCurrency {
			return nil, fmt.Errorf("wrong currency name: %s", currencyNameFilter)
		}
	}

	queryText := `
			WITH LastPricesDates AS (
				SELECT
					s.id,
					s.name,
					s.type,
					s.currency,
					max(sq.end) AS end
				FROM
					securities AS s
						LEFT OUTER JOIN security_quotes AS sq
						ON s.id = sq.security
				WHERE
					&typeCondition
					AND &currencyCondition
				GROUP BY
					s.id,
					s.name,
					s.type,
					s.currency
				)
				SELECT
					pd.id,
					pd.name,
					pd.type,
					pd.currency,
					IFNULL(sq.interv, 0) AS interv,
					sq.begin,
					sq.end,
					IFNULL(sq.open, 0.0) AS open,
					IFNULL(sq.close, 0.0) AS close,
					IFNULL(sq.high, 0.0) AS high,
					IFNULL(sq.low, 0.0) AS low
				FROM
					LastPricesDates AS pd
						LEFT OUTER JOIN security_quotes AS sq
						ON pd.id = sq.security
							AND pd.end = sq.end
				ORDER BY
				id`

	if typeNameFilter != "" {
		queryText = strings.ReplaceAll(queryText, "&typeCondition", "s.type = \""+strings.ToLower(typeNameFilter)+"\"") // safe!
	} else {
		queryText = strings.ReplaceAll(queryText, "&typeCondition", "TRUE")
	}

	if currencyNameFilter != "" {
		queryText = strings.ReplaceAll(queryText, "&currencyCondition", "s.currency = \""+strings.ToUpper(currencyNameFilter)+"\"") // safe!
	} else {
		queryText = strings.ReplaceAll(queryText, "&currencyCondition", "TRUE")
	}

	securitiesDB, err := db.Query(queryText)
	if err != nil {
		return nil, err
	}

	type securitiesDBRow struct {
		id       string
		name     string
		sType    string
		currency string
		interval int
		begin    []uint8
		end      []uint8
		open     float64
		close    float64
		high     float64
		low      float64
	}

	var res []*securities.Security

	wg := new(sync.WaitGroup)
	mu := new(sync.Mutex)

	for securitiesDB.Next() {
		var securitiesDBRowOne securitiesDBRow

		err = securitiesDB.Scan(&securitiesDBRowOne.id, &securitiesDBRowOne.name, &securitiesDBRowOne.sType, &securitiesDBRowOne.currency, &securitiesDBRowOne.interval, &securitiesDBRowOne.begin, &securitiesDBRowOne.end, &securitiesDBRowOne.open, &securitiesDBRowOne.close, &securitiesDBRowOne.high, &securitiesDBRowOne.low)
		if err != nil {
			return nil, err
		}

		wg.Add(1)

		go func(securitiesDBRowOne securitiesDBRow) {
			defer wg.Done()

			sType := securities.GetSecurityTypeFromString(securitiesDBRowOne.sType)
			cur := securities.GetSecurityCurrencyFromString(securitiesDBRowOne.currency)

			sec := securities.GetSecurity(securitiesDBRowOne.id, securitiesDBRowOne.name, sType, cur)

			strBeginDate := string(securitiesDBRowOne.begin)
			strEndDate := string(securitiesDBRowOne.end)
			if strBeginDate != "" && strEndDate != "" {
				beginDate, err := time.Parse("2006-01-02 15:04:05", strBeginDate)
				if err != nil {
					log.Fatal("can't convert database date format: " + strBeginDate)
				}

				endDate, err := time.Parse("2006-01-02 15:04:05", strEndDate)
				if err != nil {
					log.Fatal("can't convert database date format: " + strEndDate)
				}

				sQuotes := securities.SecurityQuotes{
					Interval: securities.QuotesInterval(securitiesDBRowOne.interval),
					Begin:    beginDate,
					End:      endDate,
					Open:     securitiesDBRowOne.open,
					Close:    securitiesDBRowOne.close,
					High:     securitiesDBRowOne.high,
					Low:      securitiesDBRowOne.low,
				}

				sec.SetQuotes(sQuotes)
			}

			mu.Lock()
			res = append(res, sec)
			mu.Unlock()
		}(securitiesDBRowOne)
	}

	wg.Wait()

	sort.Slice(res, func(i, j int) bool {
		return res[j].Id() > res[i].Id()
	})

	return res, nil
}

// AddSecurity adds new security to database
func AddSecurity(db *sql.DB, sec *securities.Security) error {
	seqExists, err := SecurityExists(db, sec.Id(), sec.SType())
	if err != nil {
		return err
	}

	if seqExists {
		return nil
	}

	cur := sec.Currency()
	if cur == securities.UnknownCurrency {
		cur = securities.RUB
	}
	queryText := fmt.Sprintf("INSERT INTO securities (id, name, type, currency) VALUES (\"%s\", \"%s\", \"%s\", \"%s\")", sec.Id(), sec.Name(), sec.SType(), cur)

	_, err = db.Query(queryText)
	if err != nil {
		return err
	}

	return nil
}

// AddSecurities adds a list of securities to database
func AddSecurities(db *sql.DB, sec []*securities.Security) error {
	wg := new(sync.WaitGroup)
	quitChan := make(chan bool)
	finErrChan := make(chan error)
	errChan := make(chan error)

	go collectErrors(quitChan, finErrChan, errChan)

	for _, s := range sec {
		wg.Add(1)

		go func(s *securities.Security, errChan chan error) {
			defer wg.Done()

			err := AddSecurity(db, s)

			if err != nil {
				errChan <- err
			}
		}(s, errChan)
	}

	wg.Wait()

	quitChan <- true

	err := <-finErrChan
	close(finErrChan)
	if err != nil {
		return err
	}

	return nil
}

// UpdateSecurityQuotes gets security quotes from Moscow Exchange and writes them down to database
func UpdateSecurityQuotes(db *sql.DB, sec *securities.Security, dateFrom time.Time, dateTill time.Time, interval securities.QuotesInterval) error {
	secExists, err := SecurityExists(db, sec.Id(), sec.SType())
	if err != nil {
		return err
	}

	if !secExists {
		return fmt.Errorf("security %s does not exist", sec.Id())
	}

	err = moex.GetSecurityQuotes(sec, dateFrom, dateTill, interval)
	if err != nil {
		return err
	}

	quotes := sec.QuotesOfInterval(interval)
	form := "2006-01-02 15:04:05"
	wg := new(sync.WaitGroup)

	for _, q := range *quotes {
		wg.Add(1)

		go func(q securities.SecurityQuotes) {
			defer wg.Done()

			qExist, err := SecurityQuotesExist(db, sec, q.Begin, interval)
			if err != nil {
				log.Fatal("something wrong with database query when checking if security quotes exist")
			}

			if qExist {
				// we need to delete old quotes and add new one
				// for example, yesterday we've got day quotes in the middle of the day - it looks ok but actually it's not really day quotes
				// so today we need to update it to get real day quotes for the previous day
				queryText := fmt.Sprintf("DELETE FROM security_quotes WHERE security = \"%s\" AND begin = \"%s\" AND interv = \"%d\"",
					sec.Id(), q.Begin.UTC().Format("2006-01-02 15:04:05"), interval)

				_, err = db.Query(queryText)
				if err != nil {
					log.Fatal("something wrong with database query when trying to delete old security quotes")
				}
			}

			queryText := fmt.Sprintf("INSERT INTO security_quotes (security, begin, end, interv, open, close, high, low) VALUES (\"%s\", \"%s\", \"%s\", \"%d\", \"%f\", \"%f\", \"%f\", \"%f\")",
				sec.Id(), q.Begin.UTC().Format(form), q.End.UTC().Format(form), interval, q.Open, q.Close, q.High, q.Low)

			_, err = db.Query(queryText)
			if err != nil {
				log.Fatal("something wrong with database query when trying to add security quotes")
			}
		}(q)
	}

	wg.Wait()

	return nil
}

// UpdateAllSecuritiesLastQuotes gets last quotes from Moscow Exchange for all existing in database securities (considering type and currency filters) for the day interval and writes them down to database
func UpdateAllSecuritiesLastQuotes(db *sql.DB, typeNameFilter string, currencyNameFilter string) error {
	secList, err := GetAllSecuritiesData(db, typeNameFilter, currencyNameFilter)
	if err != nil {
		return err
	}

	err = moex.GetQuotesForDate(secList, time.Now().UTC())
	if err != nil {
		return err
	}

	form := "2006-01-02 15:04:05"
	wg := new(sync.WaitGroup)

	for _, s := range secList {
		wg.Add(1)

		go func(s *securities.Security) {
			defer wg.Done()

			q := s.LastQuotes(securities.IntervalDay)

			qExist, err := SecurityQuotesExist(db, s, q.Begin, securities.IntervalDay)
			if err != nil {
				log.Fatal("something wrong with database query when checking if security quotes exist")
			}

			if qExist {
				// TODO: if security quotes exist we should also check end date and probably need to update quotes in database
				return
			}

			queryText := fmt.Sprintf("INSERT INTO security_quotes (security, begin, end, interv, open, close, high, low) VALUES (\"%s\", \"%s\", \"%s\", \"%d\", \"%f\", \"%f\", \"%f\", \"%f\")",
				s.Id(), q.Begin.UTC().Format(form), q.End.UTC().Format(form), securities.IntervalDay, q.Open, q.Close, q.High, q.Low)

			_, err = db.Query(queryText)
			if err != nil {
				log.Fatal("something wrong with database query when trying to add security quotes")
			}
		}(s)
	}

	wg.Wait()

	return nil
}

// DeleteSecurity removes security from database
func DeleteSecurity(db *sql.DB, sec *securities.Security) error {
	seqExists, err := SecurityExists(db, sec.Id(), sec.SType())
	if err != nil {
		return err
	}

	if !seqExists {
		return nil
	}

	queryText := fmt.Sprintf("DELETE FROM security_quotes WHERE security = \"%s\"", sec.Id())

	_, err = db.Query(queryText)
	if err != nil {
		return err
	}

	queryText = fmt.Sprintf("DELETE FROM securities WHERE id = \"%s\"", sec.Id())
	_, err = db.Query(queryText)
	if err != nil {
		return err
	}

	return nil
}

// CreateDatabase creates new database to work with securities
func CreateDatabase(sqlParam string, dbName string) (*sql.DB, error) {
	db, err := sql.Open("mysql", sqlParam+"/")
	if err != nil {
		return nil, err
	}

	// We should already know that database doesn't exist
	// Also we should be already sure that dbname is a good value
	_, err = db.Exec("CREATE DATABASE " + dbName)
	if err != nil {
		return nil, err
	}
	db.Close()

	// It's better to close and reopen database
	db, err = sql.Open("mysql", sqlParam+"/"+dbName)
	if err != nil {
		return nil, err
	}

	// Creating Securities table - where we keep general information about securities
	_, err = db.Exec(`
		CREATE TABLE securities(
			id VARCHAR(20) NOT NULL,
			name VARCHAR(150),
			type VARCHAR(20) NOT NULL,
			currency CHAR(3) NOT NULL,
			PRIMARY KEY (id)
		);`)
	if err != nil {
		return nil, err
	}

	// Creating Security quotes table - where we keep information about security quotes
	_, err = db.Exec(`CREATE TABLE security_quotes(
			security VARCHAR(20) NOT NULL,
			begin DATETIME NOT NULL,
			end DATETIME NOT NULL,
			interv TINYINT UNSIGNED NOT NULL,
			open DECIMAL(14,6),
			close DECIMAL(14,6),
			low DECIMAL(14,6),
			high DECIMAL(14,6),
			PRIMARY KEY (security, begin, interv),
			CONSTRAINT FK_SecurityQuotes FOREIGN KEY (security) REFERENCES securities(id)
		);`)
	if err != nil {
		return nil, err
	}

	return db, nil
}

// PutTestDataInDatabase adds some securities and quotes to database just for testing or demonstration
func PutTestDataInDatabase(db *sql.DB) error {
	var secSlice []*securities.Security

	secSlice = append(secSlice, securities.GetSecurity("GAZP", "Gazprom shares", securities.Share, securities.RUB))
	secSlice = append(secSlice, securities.GetSecurity("LKOH", "Lukoil shares", securities.Share, securities.RUB))
	secSlice = append(secSlice, securities.GetSecurity("SBER", "Sberbank shares", securities.Share, securities.RUB))
	secSlice = append(secSlice, securities.GetSecurity("SBERP", "Sberbank pref. shares", securities.Share, securities.RUB))

	secSlice = append(secSlice, securities.GetSecurity("AKGD", "Alfa Gold ETF", securities.ETF, securities.RUB))
	secSlice = append(secSlice, securities.GetSecurity("SBGD", "Sberbank Gold ETF", securities.ETF, securities.RUB))
	secSlice = append(secSlice, securities.GetSecurity("TGLD", "Tinkoff Gold ETF", securities.ETF, securities.RUB))

	err := AddSecurities(db, secSlice)
	if err != nil {
		return err
	}

	dateTill := time.Now()
	dateFrom := time.Date(2023, 9, 1, 0, 0, 0, 0, time.UTC)
	interval := securities.QuotesInterval(securities.IntervalDay)
	for _, sec := range secSlice {
		err := UpdateSecurityQuotes(db, sec, dateFrom, dateTill, interval)
		if err != nil {
			return err
		}
	}

	return nil
}
