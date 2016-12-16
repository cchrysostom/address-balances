package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/kelseyhightower/envconfig"
	_ "github.com/mattn/go-sqlite3"
)

type Payment struct {
	DestAddress    string
	PaymentAddress string
	TargetBalance  int
	PayableBalance int
	CurrentBalance int
}

type BitcoinAddress struct {
	Address  string `json:"address"`
	Balance  int    `json:"final_balance"`
	Received int    `json:"total_received"`
	Sent     int    `json:"total_sent"`
}

type BalancesConfig struct {
	PaymentsDB string `default:"payproc.db" split_words:"true"`
	LoopDelay  int    `default:"1000" split_words:"true"`
}

var (
	Trace   *log.Logger
	Info    *log.Logger
	Warning *log.Logger
	Error   *log.Logger
)

func Init(
	traceHandle io.Writer,
	infoHandle io.Writer,
	warningHandle io.Writer,
	errorHandle io.Writer) {

	Trace = log.New(traceHandle,
		"TRACE: ",
		log.Ldate|log.Ltime|log.Lshortfile)

	Info = log.New(infoHandle,
		"INFO: ",
		log.Ldate|log.Ltime|log.Lshortfile)

	Warning = log.New(warningHandle,
		"WARNING: ",
		log.Ldate|log.Ltime|log.Lshortfile)

	Error = log.New(errorHandle,
		"ERROR: ",
		log.Ldate|log.Ltime|log.Lshortfile)
}

func main() {
	Init(ioutil.Discard, os.Stdout, os.Stdout, os.Stderr)

	var config BalancesConfig
	confErr := envconfig.Process("addrbal", &config)
	if confErr != nil {
		Error.Fatal(confErr.Error())
	}

	db, err := sql.Open("sqlite3", config.PaymentsDB)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	var pymtToDest map[string]Payment
	pymtToDest = make(map[string]Payment)

	createAddressBalance := `
	  CREATE TABLE AddressBalance ( 
		  paymentAddress TEXT NOT NULL, 
		  balance INTEGER NOT NULL DEFAULT 0, 
		  balanceDate TEXT, 
		  PRIMARY KEY('paymentAddress')
	  )`

	addrBalExists := `SELECT * FROM sqlite_master WHERE name ='AddressBalance' and type='table'`

	pymtAddrSql := `
      SELECT paymentAddress, destinationAddress, targetBalance, payableBalance
        FROM PaymentAddress
        WHERE destinationAddress != ''
        ORDER BY destinationAddress, paymentAddress
	`

	existsRows, existErr := db.Query(addrBalExists)
	if existErr != nil {
		Error.Fatal(existErr)
	}
	defer existsRows.Close()

	addrBalTableExists := false
	for existsRows.Next() {
		addrBalTableExists = true
	}

	if !addrBalTableExists {
		_, createErr := db.Exec(createAddressBalance)
		if createErr != nil {
			Error.Fatal(createErr)
		}
	}

	rows, err := db.Query(pymtAddrSql)
	if err != nil {
		Error.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var pymtAddr string
		var destAddr string
		var targetBal int
		var payableBal int
		rows.Scan(&pymtAddr, &destAddr, &targetBal, &payableBal)
		pymtFwd := Payment{DestAddress: destAddr, PaymentAddress: pymtAddr, TargetBalance: targetBal, PayableBalance: payableBal}
		pymtToDest[pymtAddr] = pymtFwd
	}

	var getCount int
	Info.Println("Number of payment addresses", len(pymtToDest))

	addrBalExistsStmt, stmtErr := db.Prepare("SELECT paymentAddress, balance, balanceDate FROM AddressBalance WHERE paymentAddress = ?")
	if stmtErr != nil {
		Error.Println(stmtErr)
		return
	}
	defer addrBalExistsStmt.Close()

	insertBalStmt, insErr := db.Prepare("INSERT INTO AddressBalance (paymentAddress, balance, balanceDate) VALUES (?, ?, datetime('now'))")
	if insErr != nil {
		Error.Println(insErr)
		return
	}
	defer insertBalStmt.Close()

	updateBalStmt, updErr := db.Prepare("UPDATE AddressBalance SET balance = ?, balance = datetime('now') WHERE paymentAddress = ?")
	if updErr != nil {
		Error.Println(updErr)
		return
	}
	defer updateBalStmt.Close()

	for key, value := range pymtToDest {
		resp, err := http.Get("https://blockchain.info/address/" + key + "?format=json&limit=10")

		getCount++
		if err != nil {
			Error.Println(err)
			break
		}

		if resp.StatusCode != 200 {
			fmt.Println("Status Code", resp.StatusCode)
			fmt.Println("Status", resp.Status)
			continue
		}

		addressInfo := new(BitcoinAddress)
		readErr := json.NewDecoder(resp.Body).Decode(addressInfo)
		if readErr != nil {
			Error.Println(readErr)
			resp.Body.Close()
			continue
		}
		resp.Body.Close()

		if addressInfo.Balance > 0 {
			Info.Println("GET Count", getCount, "Payment address", addressInfo.Address, "Balance", addressInfo.Balance, "Total Received", addressInfo.Received, "Payable", value.PayableBalance)
		}

		existsRows, existsErr := addrBalExistsStmt.Query(addressInfo.Address)
		if existsErr != nil {
			Error.Println(existsErr)
			break
		}

		var existsCount int
		for existsRows.Next() {
			existsCount++
		}

		if existsCount > 0 {
			Info.Println("Updating address", addressInfo.Address, "balance", addressInfo.Balance)
			_, updateErr := updateBalStmt.Exec(addressInfo.Address, addressInfo.Balance)
			if updateErr != nil {
				Error.Println(updateErr)
				break
			}
		} else {
			Info.Println("Inserting new address", addressInfo.Address, "balance", addressInfo.Balance)
			insResult, insErr := insertBalStmt.Exec(addressInfo.Address, addressInfo.Balance)
			if insErr != nil {
				Error.Println(insErr)
				break
			}
			rowsInserted, _ := insResult.RowsAffected()
			if rowsInserted < 1 {
				Error.Println(fmt.Sprintf("Failed to insert AddressBalance for address, %s", addressInfo.Address))
			}
		}

		time.Sleep(time.Duration(config.LoopDelay) * time.Millisecond)
	}
	Info.Println("Total address count:", getCount)
}
