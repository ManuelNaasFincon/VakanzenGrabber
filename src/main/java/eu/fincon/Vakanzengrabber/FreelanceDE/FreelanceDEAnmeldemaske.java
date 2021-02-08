package eu.fincon.Vakanzengrabber.FreelanceDE;

import com.relevantcodes.extentreports.LogStatus;
import eu.fincon.Datenverarbeitung.Testdatum;
import eu.fincon.Vakanzengrabber.Base.VakanzenGrabber;
import org.openqa.selenium.WebElement;

import static eu.fincon.Logging.ExtendetLogger.LogEntry;

public class FreelanceDEAnmeldemaske extends  FreelanceDEGrabben {
    //=====================================================================
    // XPaths - Anmeldemaske öffnen
    // =====================================================================
    String strXpathLoginBTN = "//nav[@class='nav-top pull-right']//a[text() = 'Login']";
    //=====================================================================
    // XPaths - Anmeldemaske
    // =====================================================================
    String strXpathBenutzername = "//input[@id='username']";
    String strXpathPassword = "//input[@id='password']";
    String strXpathAnmeldenBTN = "//input[@id='login']";

    String strXpathFehlermeldungBeiAnmeldung = "//div[@class='alert alert-warning']";

    String strXpathHeaderLogo = "//a[@class='header-logo ']";

    String getStrXpathWelcomePanel = "//div[@class='panel-welcome']";
    public FreelanceDEAnmeldemaske(FreelanceDEGrabben pSuperclass)
    {
        super(pSuperclass.wWebseite);
        this.gObjWebDriver = pSuperclass.gObjWebDriver;
    }

    public boolean anmelden() {

        boolean blnErgebnis;
        //=====================================================================
        // Anmeldeseite öffnen
        //    TODO
        //=====================================================================
        if (wWebseite.strBenutzername.contentEquals("") || wWebseite.strPasswort.contentEquals(""))
        {
            //=====================================================================
            // Wenn eine der Variablen "strBenutzername" oder "strPasswort leer sind
            // Wird die Anmeldung übersprungen
            // =====================================================================
            return true;
        }
        if (seitentitelHolen().contains("Freelancer, Freiberufler") == false)
        {
            LogEntry(LogStatus.FAIL, "Der Seitentitel entspricht nicht dem erwarteten Titel zur Anmeldung");
            return false;
        }
        //=====================================================================
        // Das Webelement für den Loginmaskenbutton wird ermittelt und geklickt
        //
        //=====================================================================
        WebElement loginoeffnenBTN = webelementFinden(VakanzenGrabber.SelectorType.xpath, strXpathLoginBTN);
        blnErgebnis = webelementKlicken(loginoeffnenBTN);

        //=====================================================================
        // Das Webelement für den Benutzername wird ermittelt und gesetzt
        //
        //=====================================================================
        WebElement benutzernamefeld = webelementFinden(VakanzenGrabber.SelectorType.xpath, strXpathBenutzername);
        blnErgebnis = webelementSetzen(benutzernamefeld, wWebseite.strBenutzername);
        //=====================================================================
        // Das Webelement für das Passwort wird ermittelt und gesetzt
        //
        //=====================================================================
        WebElement passwortfeld = webelementFinden(VakanzenGrabber.SelectorType.xpath, strXpathPassword);
        blnErgebnis = webelementSetzen(passwortfeld, wWebseite.strPasswort);
        //=====================================================================
        // Das Webelement für den Anmeldebutton wird ermittelt und geklickt
        //
        //=====================================================================
        WebElement anmeldenBTN = webelementFinden(VakanzenGrabber.SelectorType.xpath, strXpathAnmeldenBTN);
        blnErgebnis = webelementKlicken(anmeldenBTN);

        //=====================================================================
        // Prüft das keine Fehlermeldung (Auf der Anmeldeseite angezeigt wird) und das ein WelcomePanel angezeigt wird
        // Wenn Ja -> Anmeldung war erfolgreich
        //      Nein -> Anmeldung war nicht erfolgreich
        //=====================================================================
        if (webelementFinden(SelectorType.xpath, strXpathFehlermeldungBeiAnmeldung, 1)==null /*&& webelementFinden(SelectorType.xpath, getStrXpathWelcomePanel, 1)!=null*/)
        {
            LogEntry(LogStatus.PASS, "Anmeldung war erfolgreich");
            blnErgebnis = true;
        }
        else
        {
            System.out.println("Anmeldung war Fehlerhaft");
            LogEntry(LogStatus.FAIL, "Anmeldung war Fehlerhaft");
            blnErgebnis = false;
        }
        LogEntry(LogStatus.INFO, "Logo wird geklickt...");
        WebElement webHeaderLogo = webelementFinden(SelectorType.xpath, strXpathHeaderLogo);
        webelementKlicken(webHeaderLogo);
        LogEntry(LogStatus.INFO, "Logo wurde geklickt");
        return blnErgebnis;
    }
}
