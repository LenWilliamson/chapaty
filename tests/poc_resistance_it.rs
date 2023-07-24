use std::time::Instant;

use chapaty::{
    config::{self},
    BotBuilder, MarketSimulationDataKind, MarketKind, TimeFrameKind,
};

mod common;

/**
 *
 * Testfall 1:
 *  - Zeitraum 2022-03:
 *      - Schlusskurs Freitag (2022-03-04 23:00): 39_004.73USDT
 *      - Wähle POC: 42_100.00USDT (POC so wählen das Trade nicht getriggert wird)
 *      - Short Trade findet statt (2022-03-09 09:00)
 *      - Höchster Kurs in der Woche vom Short Trade (ab Zeitpunkt des ENTRY): (2022-03-09 16:00): 42_594.06USDT => TESTEN
 *      - Niedrigster Kurs in der Woche vom Short Trade (ab Zeiptunkt des ENTRY): (2022-03-11 03:00): 38_223.60USDT => TESTEN
 *      - Schlusskurs Freitag (2022-03-11 23:00): 38_916.69USDT
 *      - Prüfe anhand der Daten der Vorwoche ob korrekterweise ein Shorttrade vorliegt
 *  - Prüfe:
 *      - Wird der Entry beim POC ausgelöst
 *      - SL1:
 *          - Wähle X = 494.07USDT => Trade läuft weiter
 *          - Wähle X = 500.00USDT => Trade läuft weiter
 *          - Wähle X = 494.06USDT => Ausgestopped, da die Bedingung "<" nicht erfüllt
 *          - Wähle X = 400.00USDT => Ausgestopped
 *      - SL2:
 *          - Berechne High & Low der Vorwoche => Testen, da Daten erst ab Dienstag vorliegen
 *              - HIGH (2022-03-02 16:00): 44_819.39USDT => Wähle HIGH da Shorttrade
 *              - LOW ist nicht am 2022-02-28 um 00-00-00 bei 37_330.23USDT da Außerhalb Zeitinterval => Testen
 *              - LOW ist 37_450.17USDT  2022-02-28 um 01-00-00
 *          - Trade läuft weiter
 *          - Setze High der Vorwoche künstlich auf 42_200.00USDT => Ausgestopped
 *      - TP1:
 *          - Wähle X = 1_000.00USDT => Gewinn = 42_100.00 - (39_004.73USDT + 1_000.00USDT)
 *          - ......nonsense: Wähle X = 4_000.00USDT => TIMEOUT mit Gewinn (macht kein Sinn, da oberhalb von POC)
 *      - TP2: Wähle X = 0.00USDT => Gewinn
 *      - TP3:
 *          - Wähle X = 500.00USDT => Gewinn = 42_100.00 - (39_004.73USDT - 500.00USDT)
 *          - Wähle X = 1_000.00USDT => TIMEOUT mit Gewinn
 *      - TIMEOUT mit Verlust fehlt => künstlich erzeugen
 *
 * Testfall 2:
 *  - Zeitraum 2022-02:
 *      - Schlusskurs Freitag (2022-02-25 23:00): 39_424.14USDT
 *      - Wähle POC: 38_100.00USDT (POC so wählen das Trade nicht getriggert wird)
 *      - Long Trade findet nicht am (2022-02-27 20:00) statt (da Wochenende) => Testen ob der Filter funktioniert, dass wir nur (Mo ab 01:00 - Fr bis 23:00) prüfen
 *      - Long Trade findet statt am (2022-02-28 01:00)
 *      - Höchster Kurs in der Woche vom Short Trade (ab Zeitpunkt des ENTRY):(2022-03-02 16:00): 45_400.39USDT => TESTEN
 *      - Niedrigster Kurs in der Woche vom Short Trade (ab Zeiptunkt des ENTRY, hier sogar gleich dem Zeitpunkt des ENTRY): (2022-02-28 01:00): 37_450.17USDT => TESTEN
 *      - Schlusskurs Freitag (2022-03-04 23:00): 39_004.73USDT
 *      - Prüfe anhand der Daten der Vorwoche ob korrekterweise ein Longtrade vorliegt
 *  - Prüfe:
 *      - Wird der Entry beim POC ausgelöst
 *      - SL1:
 *          - Wähle X = 1_000.00USDT => Trade läuft weiter
 *          - Wähle X = 649.84USDT => Trade läuft weiter
 *          - Wähle X = 649.83USDT => Ausgestopped, da die Bedingung ">" nicht erfüllt
 *          - Wähle X = 400.00USDT => Ausgestopped
 *      - SL2:
 *          - Berechne High & Low der Vorwoche => Wähle LOW da Long Trade
 *              - HIGH (2022-02-24 21:00): 39_843.00USDT
 *              - LOW ist 34_322.28USDT  2022-02-24 um 06-00-00
 *          - Trade läuft weiter
 *          - Setze Low der Vorwoche künstlich auf 37_800.00USDT => Ausgestopped
 *      - TP1:
 *          - Wähle X = 400.00USDT => GEWINN = (39_424.14 - 400) - 38_100.00
 *          - Keine weiteren Fälle hier, da TP1 die risikoaverse Version ist
 *      - TP2: Wähle X = 0.00USDT => Gewinn
 *      - TP3:
 *          - Wähle X = 5_000.00USDT => GEWINN = (39_424.14 + 5_000.00) - 38_000.00
 *          - Wähle X = 10_000.00USDT => TIMEOUT mit Gewinn
 *      - TIMEOUT mit Verlust fehlt => künstlich erzeugen
 *
 * Weiterer Testfall:
 *  - Short Trade registriert, aber kein Eintritt
 *  - Long Trade registriert, aber kein Eintritt
 *  - Jede Woche liefert resulstat mit Metadaten
 *
 * Aktuell nimmst du nur den ersten entry, was ist wenn es zwei entries gibt? Wir wirkt das auf die Margin?
 *
 * Wenn die Daten in 1Monatstabellen vorliegen muss korrekt der nächste Monat geladen werden
 *
 * Test ob höchster und niedrigster Kurs ab Zeiptunkt des ENTRY richtig bestimmt wird
 *
 * Wir brauchen einen Fall in dem der Trade nicht closed und TIMEOUT mit Gewinn / Verlust hat
 *
 * Wir müssen testen ob anhand der Daten der Vorwoche korrekt bestimmt wird ob wir einen Long oder
 * Short Trade erwarten
 *  
 * Unabhängig von Short oder Long soll es eine Funktion geben: Trade Triggerd der als Argument übergeben wird
 * ob es ein Long oder Short Trade ist. Diese prüft ob der Trade valide ist für die Kerze in der der Trade getriggerd wird.
 *  - Wir müssen Shorttrade prüfen, wenn der Eröffnungskurs unter dem POC ist
 *  - Was passiert wenn in der ersten Kerze der Trade ausgelöst wird, da POC in [Low, High], aber Low bzw. High schon ausstoppen
 */
//  https://doc.rust-lang.org/book/ch11-03-test-organization.html#integration-tests

#[tokio::test]
async fn it_test() {
    let start = Instant::now();

    let strategy = common::setup_strategy();
    let data_provider = common::setup_data_provider();
    let name = "chapaty".to_string();
    let years = vec![2022];
    let market_simulation_data = MarketSimulationDataKind::Ohlc1m;
    let markets = vec![MarketKind::EurUsdFuture];
    let time_interval = common::setup_time_interval();
    let time_frame = TimeFrameKind::Daily;
    let client = config::get_google_cloud_client().await;
    let bucket = config::GoogleCloudBucket {
        historical_market_data_bucket_name: "chapaty-ai-hdb-int".to_string(),
        cached_bot_data_bucket_name: "chapaty-ai-int".to_string(),
    };

    let bot = BotBuilder::new(strategy, data_provider)
        .with_name(name)
        .with_years(years)
        .with_markets(markets)
        .with_market_simulation_data(market_simulation_data)
        .with_time_interval(time_interval)
        .with_time_frame(time_frame)
        .with_google_cloud_client(client)
        .with_google_cloud_bucket(bucket)
        .with_save_result_as_csv(true)
        .build()
        .unwrap();

    let _ = bot.backtest().await;

    let duration = start.elapsed();
    println!("Time elapsed in streams::chapaty::backtest() is: {duration:?}");

    assert_eq!(0, 0);
}
