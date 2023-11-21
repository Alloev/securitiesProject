package securitiesSQL

import (
	"database/sql"
	"encoding/json"
	"io"
	"os"
	"securitiesModule/securities"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// getDB returns SQL database
func getDB(t *testing.T) *sql.DB {
	settingsFileName := "src\\testConf.json"

	file, err := os.Open(settingsFileName)
	if err != nil {
		t.Fatal("settings file not found")
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		t.Fatal(err.Error())
	}

	type settings struct {
		MySQL  string
		TestDB string
	}
	conf := settings{}
	err = json.Unmarshal(data, &conf)
	if err != nil {
		t.Fatal(err.Error())
	}

	sqlParam := conf.MySQL
	dbName := conf.TestDB

	db, err := sql.Open("mysql", sqlParam+"/"+dbName)
	if err != nil {
		t.Fatal(err)
	}

	err = db.Ping()
	if err != nil {
		// if database doesn't exist we'll create it
		db, err = CreateDatabase(sqlParam, dbName)
		if err != nil {
			t.Fatal(err)
		}
		err := PutTestDataInDatabase(db)
		if err != nil {
			t.Fatal(err)
		}
	}

	return db
}

func TestSecurityExists(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	// check existing security
	res, err := SecurityExists(db, "GAZP", securities.Share)
	if err != nil {
		t.Fatal(err)
	}

	if !res {
		t.Error("security GAZP not found in database")
	}

	// check not existing security
	res, err = SecurityExists(db, "AAAA", securities.Share)

	if err != nil {
		t.Fatal(err)
	}

	if res {
		t.Error("security AAAA found in database but it can't be true")
	}
}

func TestSecurityQuotesExist(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	sec := securities.GetQuickSecurity("GAZP", securities.Share)

	// check existing quotes
	res, err := SecurityQuotesExist(db, sec, time.Date(2023, 11, 1, 0, 0, 0, 0, time.UTC), securities.IntervalDay)
	if err != nil {
		t.Fatal(err)
	}

	if !res {
		t.Error("quotes for GAZP on 01.11.2023 not found in database")
	}

	// check not existing quotes
	res, err = SecurityQuotesExist(db, sec, time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC), securities.IntervalDay)
	if err != nil {
		t.Fatal(err)
	}

	if res {
		t.Error("quotes for GAZP on 01.01.1900 found in database, but it can't be true")
	}
}

func TestGetSecurityData(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	sec := securities.GetQuickSecurity("GAZP", securities.Share)

	err := GetSecurityData(db, sec)
	if err != nil {
		t.Fatal(err)
	}

	if sec.Currency() != securities.RUB {
		t.Errorf("wrong currency for GAZP - want RUB, got %s", sec.Currency())
	}

	q := sec.QuotesForDate(securities.IntervalDay, time.Date(2023, 11, 1, 23, 59, 59, 0, time.UTC))
	if q.Close == 0.0 {
		t.Errorf("no quotes for GAZP on 01.11.2023")
	} else if q.Close != 170.08 {
		t.Errorf("wrong price for GAZP on 01.11.2023 - want 170.08, got %f", q.Close)
	}
}

func TestGetAllSecuritiesData(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	sec := securities.GetQuickSecurity("GAZP", securities.Share)

	s, err := GetAllSecuritiesData(db, "share", "RUB")
	if err != nil {
		t.Fatal(err)
	}

	for _, val := range s {
		if val.Id() == sec.Id() {
			if val.Currency() != securities.RUB {
				t.Errorf("wrong currency for GAZP - want RUB, got %s", val.Currency())
			}

			/*q := val.QuotesForDate(securities.IntervalDay, time.Date(2023, 11, 1, 23, 59, 59, 0, time.UTC))
			if q.Close == 0.0 {
				t.Errorf("no quotes for GAZP on 01.11.2023")
			} else if q.Close != 170.08 {
				t.Errorf("wrong price for GAZP on 01.11.2023 - want 170.08, got %f", q.Close)
			}*/

			return
		}
	}

	t.Errorf("GAZP not found")
}

func TestAddUpdateDeleteSecurity(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	res, err := SecurityExists(db, "BLNG", securities.Share)
	if err != nil {
		t.Fatal(err)
	}

	if res {
		t.Errorf("security BLNG exists in database but it should not")
		return
	}

	sec := securities.GetQuickSecurity("BLNG", securities.Share)
	err = AddSecurity(db, sec)
	if err != nil {
		t.Fatal(err)
	}

	res, err = SecurityExists(db, "BLNG", securities.Share)
	if err != nil {
		t.Fatal(err)
	}

	if !res {
		t.Errorf("failed to add BLNG to database")
		return
	}

	err = UpdateSecurityQuotes(db, sec, time.Date(2023, 2, 1, 0, 0, 0, 0, time.UTC), time.Date(2023, 2, 28, 23, 59, 59, 0, time.UTC), securities.IntervalDay)
	if err != nil {
		t.Errorf("failed to update BLNG quotes for February 2023")
	} else {
		sec = securities.GetQuickSecurity("BLNG", securities.Share)
		err = GetSecurityData(db, sec)
		if err != nil {
			t.Errorf("failed to get BLNG quotes after update")
		} else {
			q := sec.QuotesForDate(securities.IntervalDay, time.Date(2023, 2, 14, 23, 59, 59, 0, time.UTC))
			if q.Close == 0.0 {
				t.Errorf("no quotes for BLNG on 14.02.2023")
			} else if q.Close != 15.03 {
				t.Errorf("wrong price for BLNG on 14.02.2023 - want 15.03, got %f", q.Close)
			}
		}
	}

	err = DeleteSecurity(db, sec)
	if err != nil {
		t.Fatal(err)
	}

	res, err = SecurityExists(db, "BLNG", securities.Share)
	if err != nil {
		t.Fatal(err)
	}

	if res {
		t.Errorf("failed to delete BLNG from database")
		return
	}
}

func TestUpdateAllSecuritiesLastQuotes(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	err := UpdateAllSecuritiesLastQuotes(db, "share", "RUB")
	if err != nil {
		t.Errorf("failed to update all securities last quotes")
	}

	sec := securities.GetQuickSecurity("GAZP", securities.Share)
	err = GetSecurityData(db, sec)
	if err != nil {
		t.Errorf("failed to get GAZP data after update all security quotes")
		return
	}
	q := sec.LastQuotes(securities.IntervalDay)

	if time.Now().AddDate(0, 0, -1).Truncate(time.Hour * 24).After(q.Begin) {
		t.Errorf("probably failed to update GAZP last quotes. Last quotes date - %s, want %s. Maybe it's not trade day?", q.End.Format("02.01.2006"), time.Now().AddDate(0, 0, -1).Format("02.01.2006"))
	}
}
