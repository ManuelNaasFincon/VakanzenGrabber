package eu.fincon.Datenverarbeitung;

import com.relevantcodes.extentreports.LogStatus;
import eu.fincon.Logging.ExtendetLogger;
import eu.fincon.Vakanzengrabber.Base.VakanzenGrabber;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.util.logging.Level;

public class Config {
    public static eu.fincon.Datenverarbeitung.InserateVerwalten.SpeicherTypen stSpeicherTyp;
    public static Webseite[] strarrayWebseitenListe;
    public static VakanzenGrabber.Browser bBrowser;
    public static Level lLogLevel;
    public static String strDatabasePfad;
    public static String strDatabaseName;
    public static String strZielTabellenName;
    public static String strZielOrdnerReport;
    public static boolean blnHeadless = true;
    public static void init()
    {
        ExtendetLogger.CreateChild("Konfiguration laden");
        DatabaseLaden();
        WebseitenlisteLaden();
        SpeicherzielLaden();
        ReportZielLaden();
        setBrowser();
        setLogLevel();
        setHeadless();
        ExtendetLogger.AppendChild();
    }
    public static String ReportZielLaden() {
        strZielOrdnerReport = getWertAusConfig("", "ZielOrdnerReport");
        return strZielOrdnerReport;
    }
    public static void SpeicherzielLaden()
    {
        String strSpeicherziel = getWertAusConfig("", "Speicherziel").toLowerCase();
        switch (strSpeicherziel)
        {
            case "csv":
                stSpeicherTyp = InserateVerwalten.SpeicherTypen.csv;
                break;
            case "sqlite":
                stSpeicherTyp = InserateVerwalten.SpeicherTypen.sqllite;
                break;
            default:
                stSpeicherTyp = InserateVerwalten.SpeicherTypen.sqllite;
                break;
        }
        ExtendetLogger.LogEntry(LogStatus.INFO, "Speichertyp - " + stSpeicherTyp.toString() + " wurde geladen");
    }
    public static void DatabaseLaden()
    {
        strDatabasePfad = getWertAusConfig("", "DatabasePfad");
        ExtendetLogger.LogEntry(LogStatus.INFO, "Database Pfad wurde geladen - " + strDatabasePfad);
        strDatabaseName = getWertAusConfig("", "DatabaseName");
        ExtendetLogger.LogEntry(LogStatus.INFO, "Database Name wurde geladen - " + strDatabaseName);
        strZielTabellenName = getWertAusConfig("", "DatabaseTabellenname");
        ExtendetLogger.LogEntry(LogStatus.INFO, "Zieltabellenname wurde geladen - " + strZielTabellenName);
    }
    public static void WebseitenlisteLaden()
    {
        //=====================================================================
        // Holt den das Root-Element der Config.XML
        // =====================================================================
        Element rootElement = getRootElement();

        if (rootElement==null)
        {
            ExtendetLogger.LogEntry(LogStatus.FATAL, "Config.XML konnte nicht geladen werden");
            return;
        }
        //=====================================================================
        // "selektiert" alle Elemente vom Typ "Seite"
        // =====================================================================
        NodeList eSeitenElemente = rootElement.getElementsByTagName("Seite");

        strarrayWebseitenListe = new Webseite[eSeitenElemente.getLength()];
        for (int i=0; i<eSeitenElemente.getLength(); i++) {
            String strName = eSeitenElemente.item(i).getAttributes().getNamedItem("name").getNodeValue();
            strarrayWebseitenListe[i]=new Webseite(strName);
            System.out.println(strName);
        }
    }
    public static String strConfigFilePath = "config.xml";
    public static String getBenutzername(String pstrSeite)
    {
        return getWertAusConfig(pstrSeite, "benutzername");
    }
    public static String getPasswort(String pstrSeite)
    {
        return getWertAusConfig(pstrSeite, "passwort");
    }
    public static String getURL(String pstrSeite)
    {
        return getWertAusConfig(pstrSeite, "url");
    }
    public static void setBrowser() {

        //=====================================================================
        // Holt den Wert für das Element Browser aus der Config und setzt
        // die statische Variable bBrowser auf den entsprechenden Wert
        // =====================================================================
        String strBrowserAusConfig = getWertAusConfig("", "Browser");
        switch (strBrowserAusConfig.toLowerCase()) {
            case "chrome":
                bBrowser = VakanzenGrabber.Browser.Chrome;
                break;
            default:
                bBrowser = VakanzenGrabber.Browser.Chrome;
        }
    }
    public static void setLogLevel()
    {
        //=====================================================================
        // Holt den Wert für das Element Browser aus der Config und setzt
        // die statische Variable bBrowser auf den entsprechenden Wert
        // =====================================================================
        String strLogLevelAusConfig = getWertAusConfig("", "LogLevel");
        switch (strLogLevelAusConfig.toLowerCase())
        {
            case "all":
                lLogLevel = Level.ALL;
                break;
            case "info":
                lLogLevel = Level.INFO;
                break;
            case "config":
                lLogLevel = Level.ALL;
                break;
            case "warning":
                lLogLevel = Level.WARNING;
                break;
            case "fine":
                lLogLevel = Level.FINE;
                break;
            case "finer":
                lLogLevel = Level.FINER;
                break;
            case "finest":
                lLogLevel = Level.FINEST;
                break;
            case "off":
                lLogLevel = Level.OFF;
                break;
            default:
                lLogLevel = Level.ALL;
        }
    }
    static void setHeadless()
    {
        //=====================================================================
        // Holt den Wert für das Element Headless aus der Config und setzt
        // die statische Variable bBrowser auf den entsprechenden Wert
        // =====================================================================
        String strHeadlessAusConfig = getWertAusConfig("", "Headless");
        if (strHeadlessAusConfig.toLowerCase().contentEquals("true") || strHeadlessAusConfig.toLowerCase().contentEquals("ja"))
        {
            blnHeadless = true;
        }
        else
        {
            blnHeadless = false;
        }
    }
    static Element getRootElement()
    {
        //=====================================================================
        // Prüfen ob die Config-Datei vorhanden ist
        // =====================================================================
        if (!new File(strConfigFilePath).exists())
        {
            ExtendetLogger.LogEntry(LogStatus.INFO,"Datei nicht gefunden - " + strConfigFilePath);
            return null;
        }
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = null;
        try {
            builder = factory.newDocumentBuilder();
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
            ExtendetLogger.LogEntry(LogStatus.FATAL,"Exception: " + e.getMessage());
            return null;
        }
        Document document = null;
        try {
            //=====================================================================
            // Parsed die Datei in dem übergebenen Pfad zu XML
            // =====================================================================
            ExtendetLogger.LogEntry(LogStatus.INFO,"Dokument " + strConfigFilePath + " wird geladen...");
            document = builder.parse(new InputSource(strConfigFilePath));
            ExtendetLogger.LogEntry(LogStatus.INFO,"Dokument " + strConfigFilePath + " wurde geladen");
        } catch (SAXException e) {
            e.printStackTrace();
            ExtendetLogger.LogEntry(LogStatus.FATAL,"Exception: " + e.getMessage());
            return null;
        } catch (IOException e) {
            e.printStackTrace();
            ExtendetLogger.LogEntry(LogStatus.FATAL,"Exception: " + e.getMessage());
            return null;
        }

        //=====================================================================
        // Selektiert das überste Element der XML
        //        // =====================================================================
        return document.getDocumentElement();
    }
    //=====================================================================
    // Öffnet eine XML Datei aus dem Pfad der Globalen Variablen ""
    // Und gibt einen Wert für ein Attribut zurück der übergeben Parameter "name" zurück
    // =====================================================================
    private static String getWertAusConfig(String pstrSeite, String pstrAttributName)
    {
        //=====================================================================
        // XML auslesen
        // =====================================================================
        String strWert = "";
        Element rootElement = getRootElement();
        if (rootElement==null)
        {
            return "";
        }
        if(!pstrSeite.equalsIgnoreCase("")) {

            //=====================================================================
            // Ändert die Übergabeparameter in Kleinbuchstaben zur besser vergleichbarkeit
            // =====================================================================
            pstrSeite = pstrSeite.toLowerCase();
            pstrAttributName = pstrAttributName.toLowerCase();

            //=====================================================================
            // "selektiert" alle Elemente vom Typ "Seite"
            // =====================================================================
            NodeList eSeitenElemente = rootElement.getElementsByTagName("Seite");

            //=====================================================================
            // Läuft durch alle "Seite"-Elemente und "sucht" die Webseite im Attribut "name"
            // Wenn das Element gefunden wurde, wird der Werte für das übergebene Attribut als Rückgabewert gesetzt
            // =====================================================================
            for (int i = 0; i < eSeitenElemente.getLength(); i++) {

                //=====================================================================
                // Sucht das Element "Seite" mit dem Attribut "name" = pstrSeite
                // =====================================================================
                if (eSeitenElemente.item(i).getAttributes().getNamedItem("name").getNodeValue().toLowerCase().contentEquals(pstrSeite)) {
                    String strValue = "";
                    //=====================================================================
                    // Sucht das übergebene Attribut in "Seite" mit dem Attribut "name" = pstrSeite
                    // =====================================================================
                    for (int j = 0; j < eSeitenElemente.item(i).getAttributes().getLength(); j++) {
                        if (eSeitenElemente.item(i).getAttributes().item(j).getNodeName().toLowerCase().contentEquals(pstrAttributName)) {
                            ExtendetLogger.LogEntry(LogStatus.INFO,"Wert aus Attribut (" + pstrAttributName + ") wird aus XML geladen...");
                            strValue = eSeitenElemente.item(i).getAttributes().item(j).getNodeValue();
                            ExtendetLogger.LogEntry(LogStatus.INFO,"Wert " + strValue + " aus Attribut (" + pstrAttributName + ") wurde aus XML geladen");
                            break;
                        }
                    }
                    return strValue;
                }
            }
        }
        else
        {
            //=====================================================================
            // "selektiert" das Element welches als Attribut übergeben wurde (nur den ersten Eintrag, sofern mehrere
            // gefunden wurden
            // ====================================================================
            strWert = rootElement.getElementsByTagName(pstrAttributName).item(0).getTextContent();
        }
        return strWert;
    }
}
