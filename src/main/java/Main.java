import com.relevantcodes.extentreports.LogStatus;
import eu.fincon.Datenverarbeitung.*;
import eu.fincon.Logging.ExtendetLogger;

import java.util.List;
import java.util.logging.Level;

public class Main {
    public static void main(String[] args) {
        //=====================================================================
        // Logger setup
        // =====================================================================
        ExtendetLogger.setup("Vakanzengrabber");
        //=====================================================================
        // Eine Liste (Typ Testdatum) wird aus der übergebenen Datei erstellt
        // =====================================================================
        List<Testdatum> lTestdatumListe = Datentreiber.getTestdatenEXCEL();
        //=====================================================================
        // Config Laden
        // =====================================================================
        Config.init();
        //=====================================================================
        // Tests werden ermittelt und geloggt
        // =====================================================================
        ExtendetLogger.LogEntry(LogStatus.INFO,"Es werden "+Config.strarrayWebseitenListe.length+" Webseiten geprüft");
        ExtendetLogger.LogEntry(LogStatus.INFO,"Es werden "+lTestdatumListe.size()+" Testdaten pro Seite geprüft");
        //=====================================================================
        // Es wird eine Schleif über alle Einträge des Testdatentreibers gelaufen
        // ====================================================================
        for (Webseite wWebseite : Config.strarrayWebseitenListe) {
            for (Testdatum tTestdatum : lTestdatumListe) {
                ExtendetLogger.CreateChild(wWebseite.eSeite.toString() + "_" + tTestdatum.strSuchbegriff);
                ExtendetLogger.LogEntry(LogStatus.INFO,"Webseite - "+wWebseite.strURL+
                        " mit dem Suchbegriff -"+tTestdatum.strSuchbegriff+" wird ausgeführt...");
                //=====================================================================
                // Die Seitenspezifischen Methoden aus der jeweiligen Klasse werden aufgerufen
                // ====================================================================
                wWebseite.VakanzenObject.browserVorbereiten();
                wWebseite.VakanzenObject.seiteOeffnen();
                wWebseite.VakanzenObject.benutzerAnmelden();
                wWebseite.VakanzenObject.sucheDurchfuehren(tTestdatum);
                wWebseite.VakanzenObject.suchlisteSichern();
                wWebseite.VakanzenObject.seiteSchließen();
                ExtendetLogger.AppendChild();
            }
        }
        //=====================================================================
        // Datenbank Zieltabelle aktualisieren
        // =====================================================================
        /*
        ToDo Funktion befindet sich aktuell im Aufbau
        if (!DatenbankVerwalten.ZieltabelleVorbereiten())
            ExtendetLogger.LogEntry(LogStatus.FATAL,"Bei dem erzeugen der Zieltabelle ist ein Fehler aufgetreten");
         */
        //=====================================================================
        // Logger finish
        // =====================================================================
        ExtendetLogger.finish();
    }
}
