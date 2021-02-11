package eu.fincon.Datenverarbeitung;

import com.relevantcodes.extentreports.LogStatus;
import eu.fincon.Logging.ExtendetLogger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class DatenbankVerwalten {
    public static List<String> listTabellenNamen = new ArrayList<String>();
    // Connection zu einer Datenbank erstellen
    public static Connection connectToSQLLiteDatabase()
    {
        // SQLite connection string
        String url = Config.strDatabasePfad + Config.strDatabaseName;
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url);
        } catch (SQLException e) {
            ExtendetLogger.LogEntry(LogStatus.ERROR, "Failed to connect to Database - " + url);
        }
        return conn;
    }
    // Neue Tabelle anlegen

    public static String createNewTable(Connection conn, String pstrTabellenName, boolean blnAddDate)
    {
        String url = Config.strDatabasePfad + Config.strDatabaseName;
        String strTabellenName = pstrTabellenName;
        if (blnAddDate)
            strTabellenName = strTabellenName + LocalDateTime.now().format(DateTimeFormatter.ofPattern("_yyyy_MM_dd"));
        dropExistingTable(conn, strTabellenName);
        // SQL statement for creating a new table
        String sql = Inserat.getSQLiteCreateTable(strTabellenName);

        try
        {
            Statement stmt = conn.createStatement();
            // create a new table
            stmt.execute(sql);
            ExtendetLogger.LogEntry(LogStatus.INFO, sql);
            conn.commit();
        } catch (SQLException e) {
            ExtendetLogger.LogEntry(LogStatus.ERROR, "Failed to create Table - " + sql);
            ExtendetLogger.LogEntry(LogStatus.ERROR, e.getMessage());
        }
        if (!listTabellenNamen.contains(strTabellenName) && !strTabellenName.contentEquals(Config.strZielTabellenName))
        {
            listTabellenNamen.add(strTabellenName);
        }
        return strTabellenName;
    }
    // Tabelle Droppen
    public static void dropExistingTable(Connection conn, String pstrTabellenName)
    {
        if (listTabellenNamen.contains(pstrTabellenName))
        {
            listTabellenNamen.remove(pstrTabellenName);
        }
        String sql = "DROP TABLE IF EXISTS " + pstrTabellenName;
        try
        {
            Statement stmt = conn.createStatement();
            // create a new table
            stmt.execute(sql);
            ExtendetLogger.LogEntry(LogStatus.INFO, sql);
            conn.commit();
        } catch (SQLException e) {
            ExtendetLogger.LogEntry(LogStatus.ERROR, "Failed to Drop Table - " + sql);
            ExtendetLogger.LogEntry(LogStatus.ERROR, e.getMessage());
        }
    }
    // Inserat in Tabelle einfügen
    public static void insertIntoSQLite(Inserat piInserat, Connection pConnection, String pstrTabellenname, int pintID) {


        //String sql = "INSERT INTO "+ pstrTabellenname +" (" + Inserat.getSQLiteSpalten() + ") VALUES(" + piInserat.getInseratStringSQLite() + ")";

        //
        String[] strsplittedValues = piInserat.getInseratStringSQLite().split("\",\"");
        // Prepared Statement wird angelegt
        String sql = getBaseInsertString(pstrTabellenname, strsplittedValues);
        String ausgabe = "";

        PreparedStatement stmt = getPreparedStatement(pConnection, sql);
        // Die Schleife läuft über die Liste an gesplitteten Werten aus dem Inserat und ersetzt in dem
        // prepared Statement die "?" mit den jeweiligen Werten
        // Da die Werte unmittelbar als String interpretiert werden, ist keine SQL-Injection möglich
        ExtendetLogger.LogEntry(LogStatus.INFO, "Setting Insert Values in Statement");
        for (int i = 1; i <= strsplittedValues.length; i++) {
            ausgabe = "Replacing " + i + " with - " + strsplittedValues[i - 1];
            System.out.println(ausgabe);
            ExtendetLogger.LogEntry(LogStatus.INFO, ausgabe);
            try {
                // Index I wird mit dem entsprechenden Werte aus der Liste ersetzt
                stmt.setString(i, strsplittedValues[i - 1].trim());
            } catch (SQLException e) {
                ExtendetLogger.LogEntry(LogStatus.FATAL, "Error setting String # " + sql + " to " + strsplittedValues[i - 1].trim());
                System.out.println("Error setting String # " + sql + " to " + strsplittedValues[i - 1].trim());
                e.printStackTrace();
            }
        }
        if (stmt != null)
            ExecuteStatement(pConnection, stmt, sql);
        else {
            ExtendetLogger.LogEntry(LogStatus.FATAL, "Error Executing Statement: Statement = null");
            System.out.println("Error Executing Statement: Statement = null");
        }
    }

    @Nullable
    private static PreparedStatement getPreparedStatement(Connection pConnection, String sql) {
        ExtendetLogger.LogEntry(LogStatus.INFO, "Creating SQL Insert Statement");
        PreparedStatement stmt = null;
        try {
            stmt = pConnection.prepareStatement(sql);
        } catch (SQLException e) {
            ExtendetLogger.LogEntry(LogStatus.FATAL, "Error preparing Statement - " + sql);
            System.out.println("Error preparing Statement - " + sql);
            e.printStackTrace();
        }
        return stmt;
    }

    private static void ExecuteStatement(Connection pConnection, PreparedStatement stmt, String sql) {
        try {
            ExtendetLogger.LogEntry(LogStatus.INFO, "Statement - " + stmt.toString());
            System.out.println("Insert Statement - " + stmt.toString());
            // Statement wird ausgeführt
            stmt.execute();
            stmt.close();
            pConnection.commit();
            ExtendetLogger.LogEntry(LogStatus.PASS, "Statement Executed");

        } catch (SQLException e) {
            ExtendetLogger.LogEntry(LogStatus.ERROR, "Failed to Execute Statement - " + sql);
            ExtendetLogger.LogEntry(LogStatus.ERROR, e.getMessage());
            e.printStackTrace();
        }
    }

    public static boolean ZieltabelleVorbereiten() {
        ExtendetLogger.LogEntry(LogStatus.INFO, "Zieltabelle " + Config.strZielTabellenName + " wird vorbereitet");
        Connection conn = connectToSQLLiteDatabase();
        String sql = "INSERT INTO " + Config.strZielTabellenName + "\n[UNIONS]";
        String strUNIONS = "";
        boolean blnFirstSelect = true;

        // Neue Zieltabelle wird angelegt (Bereits bestehende Tabelle wird vorher entfernt)
        createNewTable(conn, Config.strZielTabellenName, false);

        // Unions werden verkettet
        for (String strTabelle : listTabellenNamen) {
            String strTabellenSelect = "SELECT " + "*" + " FROM " + strTabelle;
            if (blnFirstSelect)
                strUNIONS = strTabellenSelect;
            else
                strUNIONS = strUNIONS + "\nUNION\n" + strTabellenSelect;
            blnFirstSelect = false;
        }
        // Ersetzt den Platzhalter mit den zusammengeführten UNION/SELECTS
        sql = sql.replace("[UNIONS]", strUNIONS);
        // Statement wird erzeugt
        PreparedStatement stmt = getPreparedStatement(conn, sql);
        ExtendetLogger.LogEntry(LogStatus.INFO, "Statement to be Executed\n" + sql);
        // Statement wird gegen die Datenbank ausgeführt

        if (stmt != null) {
            ExecuteStatement(conn, stmt, sql);
            return true;
        } else {
            ExtendetLogger.LogEntry(LogStatus.FATAL, "Error Executing Statement: Statement = null");
            System.out.println("Error Executing Statement: Statement = null");
            return false;
        }
    }
    @NotNull
    private static String getBaseInsertString(String pstrTabellenname, String[] strsplittedValues) {
        int intNumberOfValues = strsplittedValues.length;
        String strValuesPlaceHolder = "";
        for (int i=0;i<intNumberOfValues;i++)
        {
            if (i>0)
                strValuesPlaceHolder = strValuesPlaceHolder + ",?";
            else
                strValuesPlaceHolder = "?";
        }
        return "INSERT INTO "+ pstrTabellenname +" (" + Inserat.getSQLiteSpalten() + ") VALUES("+strValuesPlaceHolder+")";
    }
}
