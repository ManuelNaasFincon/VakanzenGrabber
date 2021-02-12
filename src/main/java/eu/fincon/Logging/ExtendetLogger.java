package eu.fincon.Logging;
import com.relevantcodes.extentreports.ExtentReports;
import com.relevantcodes.extentreports.ExtentTest;
import com.relevantcodes.extentreports.LogStatus;
import com.relevantcodes.extentreports.NetworkMode;
import eu.fincon.Datenverarbeitung.Config;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.logging.*;

public class ExtendetLogger {
    static Logger logger = null;
    static ExtentTest ExtendTestLogParent = null;
    static ExtentTest ExtendTestLog = null;
    static ExtentReports Extendlogger = null;
    static String strLogFilePath = "Log.html";
    public enum LogTypes{
        severe,
        warning,
        info,
        config,
        fine,
        finer,
        finest
    }

    //=====================================================================
    // Initialisiert den Logger
    // =====================================================================
    public static void setup(String pstrName)
    {
        String strPfad = Config.ReportZielLaden()+"ext_"+strLogFilePath;
        //logger = Logger.getLogger(pstrName);
        System.out.println("Zielordner Report: " + strPfad);
        Extendlogger = new ExtentReports(strPfad, NetworkMode.OFFLINE);
        ExtendTestLogParent = Extendlogger.startTest(pstrName);

        //createFileHandler();
        setLogLevel(Level.ALL);
    }
    //=====================================================================
    // Schließt den Logger ab
    // =====================================================================
    public static void finish()
    {
        // ending test
        Extendlogger.endTest(ExtendTestLogParent);

// writing everything to document
        Extendlogger.flush();
    }

    private static void createFileHandler() {
        FileHandler fh;
        File tmpLogFile = new File(strLogFilePath);
        if (tmpLogFile.exists())
        {
            try {
                tmpLogFile.delete();
            } catch (SecurityException e) {
                e.printStackTrace();
            }
        }
        try {
            // This block configure the logger with handler and formatter
            fh = new FileHandler(strLogFilePath);
            // Setzt den Formatter auf die CustomFormatter Klasse
            fh.setFormatter(new CustomFormatter());
            // Fügt den Handler (FileHandler) dem Logger hinzu
            logger.addHandler(fh);

        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    //=====================================================================
    // erzeugt ein Child und gibt dieses zurück
    // =====================================================================
    public static void CreateChild(String pstrChildName)
    {
        ExtendTestLog = Extendlogger.startTest(pstrChildName);
    }
    //=====================================================================
    // fügt ein Child dem Parent hinzu
    // =====================================================================
    public static void AppendChild()
    {
        ExtendTestLogParent.appendChild(ExtendTestLog);
    }
    //=====================================================================
    // Erstellt einen Logeintrag
    // =====================================================================
    public static void LogEntry(LogTypes pltType, String pstrMessage)
    {
        String strClassName = StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE).getCallerClass().toString();
        String strMethodName = Thread.currentThread().getStackTrace()[2].getMethodName();

        String strMessage = pstrMessage + ";" + strClassName + ";" + strMethodName;

        switch (pltType)
        {
            case severe:
                logger.severe(strMessage);
                break;
            case warning:
                logger.warning(strMessage);
                break;
            case info:
                logger.info(strMessage);
                break;
            case config:
                logger.config(strMessage);
                break;
            case fine:
                logger.fine(strMessage);
                break;
            case finer:
                logger.finer(strMessage);
                break;
            case finest:
                logger.finest(strMessage);
                break;
        }
    }
    //=====================================================================
    // Erstellt einen Logeintrag
    // =====================================================================
    public static void LogEntry(LogStatus plsStatus, String pstrMessage)
    {
        if (ExtendTestLog==null)
            return;
        String strClassName = StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE).getCallerClass().toString();
        String strMethodName = Thread.currentThread().getStackTrace()[2].getMethodName();

        String strExtMessage = "Klasse: " + strClassName + "<br>Methode:" + strMethodName + "<br>Nachricht:" + pstrMessage;

        switch (plsStatus)
        {
            case ERROR:
                ExtendTestLog.log(LogStatus.ERROR, strExtMessage);
                break;
            case WARNING:
                ExtendTestLog.log(LogStatus.WARNING, strExtMessage);
                break;
            case INFO:
                ExtendTestLog.log(LogStatus.INFO, strExtMessage);
                break;
            case FAIL:
                ExtendTestLog.log(LogStatus.FAIL, strExtMessage);
                break;
            case FATAL:
                ExtendTestLog.log(LogStatus.FATAL, strExtMessage);
                break;
            case SKIP:
                ExtendTestLog.log(LogStatus.SKIP, strExtMessage);
                break;
            case PASS:
                ExtendTestLog.log(LogStatus.PASS, strExtMessage);
                break;
        }
    }
    public static void setLogLevel(Level pLogLevel)
    {
        logger.setLevel(pLogLevel);
    }
}
