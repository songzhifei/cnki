import java.io.Serializable;

public class CustomizedKeyWord implements Serializable{

    private double Weight=0D;

    private String DateTime;

    public CustomizedKeyWord() {

    }

    public CustomizedKeyWord(double weight, String dateTime) {
        this.Weight = weight;
        DateTime = dateTime;
    }

    public double getWeight() {
        return Weight;
    }

    public String getDateTime() {
        return DateTime;
    }

    public void setWeight(double weight) {
        this.Weight = weight;
    }

    public void setDateTime(String dateTime) {
        DateTime = dateTime;
    }

    @Override
    public String toString() {
        return "{" +
                "\"weight\":" + Weight +
                ",\"dateTime\":\"" + DateTime + '\"' +
                '}';
    }
}
