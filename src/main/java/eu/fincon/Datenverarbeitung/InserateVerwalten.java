package eu.fincon.Datenverarbeitung;

import com.relevantcodes.extentreports.LogStatus;
import eu.fincon.Logging.ExtendetLogger;
import org.jetbrains.annotations.NotNull;

import java.io.*;
// SQL Imports
import java.sql.*;
// Time Imports
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
// List Import
import java.util.List;

public class InserateVerwalten {
    public enum SpeicherTypen{
        csv,
        txt,
        sqllite
    }
    //=====================================================================
    // Der Text des Elementes wird versucht auszulesen
    // =====================================================================
    public String ExportPfad = "export";
    List<Inserat> lInserate;

    // Database Functions

    // End Database Functions
    public InserateVerwalten(List<Inserat> piwInserate)
    {
        //=====================================================================
        // Die bei der Instanziierung übergebene Liste wird in der Klasse gesichert
        // =====================================================================
        lInserate = piwInserate;
    }
    public void inserateSichern(SpeicherTypen pstSpeicherTyp, String pstrTabellenName)
    {
        if (pstSpeicherTyp == SpeicherTypen.csv || pstSpeicherTyp == SpeicherTypen.txt)
            fileOutput(pstSpeicherTyp);
        else if (pstSpeicherTyp == SpeicherTypen.sqllite)
            sqliteOutput(pstrTabellenName);

    }
    private void sqliteOutput(String pstrTabellenName)
    {
        Connection conn = DatenbankVerwalten.connectToSQLLiteDatabase();
        try {
            // Damit wird die AutoCommit deaktiviert
            // Dieser führte dazu, dass mit jedem Statement ein Commit durchgeführt wurde - Was zu einer Exception geführt hat
            // Der Commit wird "manuell" im Code nach dem Statement ausgeführt
            conn.setAutoCommit(false);
        }
        catch (SQLException e)
        {
            ExtendetLogger.LogEntry(LogStatus.INFO, "Failed to Set AutoCommitMode");
        }
        pstrTabellenName = DatenbankVerwalten.createNewTable(conn, pstrTabellenName, true);
        ExtendetLogger.LogEntry(LogStatus.INFO, "Inserate werden gesichert... ");
        int intIndex = 1;
        for (Inserat iInserat:lInserate) {
            ExtendetLogger.LogEntry(LogStatus.INFO, "Inserat sichern: " + iInserat.toString());
            DatenbankVerwalten.insertIntoSQLite(iInserat, conn, pstrTabellenName, intIndex);
            intIndex++;
        }
    }
    private void fileOutput(SpeicherTypen pstSpeicherTyp) {
        OutputStream osExportFile = null;
        File fileExportFile = null;
        String formattedDateTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy_MM_dd_HH_mm_ss"));
        String strAbsoluterPfad = ExportPfad +"_"+ formattedDateTime +"." + pstSpeicherTyp.toString();
        ExtendetLogger.LogEntry(LogStatus.INFO, "Dateipfad für Sicherung = " + strAbsoluterPfad);

        try {
            fileExportFile = new File(strAbsoluterPfad);
            if (fileExportFile.exists()) {
                ExtendetLogger.LogEntry(LogStatus.FATAL, "Die Datei ist bereits vorhanden und wird vor dem Sichern entfernt. Speichertyp = " + pstSpeicherTyp.toString());
                fileExportFile.delete();
            }
            fileExportFile.createNewFile();
        }
        catch (Exception e) {
            ExtendetLogger.LogEntry(LogStatus.FATAL, "Die Export-Datei konnte nicht erzeugt werden");
            return;
        }

        try {
            osExportFile = new FileOutputStream(strAbsoluterPfad);
        } catch (Exception e) {
            ExtendetLogger.LogEntry(LogStatus.FATAL, "Exception in CSV schreiben  - " + e.getMessage());
        }
        inCSVDateiSchreiben (osExportFile, "#;"+ Inserat.getCSVSpalten());
        //=====================================================================
        // Es wird mittels Schleife über jeden Eintrag der Inserate Liste gegangen
        // =====================================================================
        int index = 1;
        for( Inserat i: lInserate )
        {
            switch (pstSpeicherTyp) {
                case csv:
                    ExtendetLogger.LogEntry(LogStatus.INFO, "Speichertyp wird verwendet -> " + pstSpeicherTyp.toString());
                    String strInseratStringCSV = i.getInseratStringCSV();
                    inCSVDateiSchreiben(osExportFile, Integer.toString(index)+";"+strInseratStringCSV);
                    break;
                case txt:
                    // Weitere Exporte möglich
                    break;
                default:
                    ExtendetLogger.LogEntry(LogStatus.WARNING, "Speichertyp wurde nicht gefunden - " + pstSpeicherTyp.toString());
                    return;
            }
            index ++;
        }
        try {
            osExportFile.flush();
            osExportFile.close();
        } catch (Exception e) {
            ExtendetLogger.LogEntry(LogStatus.FATAL, "Exception in CSV schreiben  - " + e.getMessage());
        }
    }

    public void inCSVDateiSchreiben (OutputStream fileExportFile, String pstrInseratStringCSV)
    {
        PrintWriter pwExport = null;
        String strText = "";

        //=====================================================================
        // Der Übergebene String wird für die Ausgabe vorbereitet
        // Zeilenumbrüche werden für die CSV mit HTML-Tags ersetzt
        // =====================================================================
        strText = pstrInseratStringCSV.replace("\r\n","<br>");
        strText = strText.replace("\n","<br>");
        strText = strText.concat("\r\n");
        //=====================================================================
        // Der Aufgearbeitete String wird in die Datei geschrieben
        // Flush garantiert das der Buffer in die Datei geschrieben wird.
        // =====================================================================
        try {
            pwExport = new PrintWriter(new OutputStreamWriter(fileExportFile, "UTF-8"));
            ExtendetLogger.LogEntry(LogStatus.INFO, "Text wird in die Datei geschrieben...");
            pwExport.append(strText);
            ExtendetLogger.LogEntry(LogStatus.INFO, "Text wurde in die Datei geschrieben.<br>" + strText);
            pwExport.flush();
        } catch (Exception e) {
            ExtendetLogger.LogEntry(LogStatus.FATAL, "Exception in CSV schreiben  - " + e.getMessage());

            System.out.println();
        }
        ExtendetLogger.LogEntry(LogStatus.PASS, "Text erfolgreich in die Datei geschrieben  - " + strText);

    }
}