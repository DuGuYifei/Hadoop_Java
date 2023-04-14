package mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class VisitoreeTuple implements WritableComparable<VisitoreeTuple> {

    private String firstName = "";
    private String midName = "";
    private String lastName = "";

    private String visitoreeLastName = "";
    private String visitoreeFirstName = "";



    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        if(firstName != null) this.firstName = firstName;
        else this.firstName = "";
    }

    public String getMidName() {
        return midName;
    }

    public void setMidName(String midName) {
        if(midName != null) this.midName = midName;
        else this.midName = "";
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        if(lastName != null) this.lastName = lastName;
        else this.lastName = "";
    }

	public String getVisitoreeLastName(){
        return visitoreeLastName;
    }

    public void setVisitoreeLastName(String lastName) {
        if(lastName != null) this.visitoreeLastName = lastName;
        else this.visitoreeLastName = "";
    }

    public String getVisitoreeFirstName(){
        return visitoreeFirstName;
    }

    public void setVisitoreeFirstName(String firstName) {
        if(firstName != null) this.visitoreeFirstName = firstName;
        else this.visitoreeFirstName = "";
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(firstName);
        out.writeUTF(midName);
        out.writeUTF(lastName);
        out.writeUTF(visitoreeLastName);
        out.writeUTF(visitoreeFirstName);
    }

    public void readFields(DataInput in) throws IOException {
        firstName = in.readUTF();
        midName = in.readUTF();
        lastName = in.readUTF();
        visitoreeLastName = in.readUTF();
        visitoreeFirstName = in.readUTF();
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + firstName.hashCode();
        result = 31 * result + lastName.hashCode();
        result = 31 * result + midName.hashCode();
        result = 31 * result + visitoreeLastName.hashCode();
        result = 31 * result + visitoreeFirstName.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof VisitoreeTuple)) return false;
        VisitoreeTuple visitoree = (VisitoreeTuple) o;

        return visitoree.getFirstName().equals(firstName) &&
                visitoree.getMidName().equals(midName) &&
                visitoree.getLastName().equals(lastName) &&
                visitoree.getVisitoreeLastName().equals(visitoreeLastName) &&
                visitoree.getVisitoreeFirstName().equals(visitoreeFirstName);
    }

    @Override
    public String toString() {
        return firstName + " " + midName + " " + lastName + "; " + visitoreeLastName + " " + visitoreeFirstName;
    }

    public int compareTo(VisitoreeTuple v) {
        int last = lastName.compareTo(v.getLastName());
        if(last!=0) return last;
        int first = firstName.compareTo(v.getFirstName());
        if(first!=0) return first;
        int mid = midName.compareTo(v.getMidName());
        if(mid!=0) return mid;
        last = visitoreeLastName.compareTo(v.getVisitoreeLastName());
        if(last!=0) return last;
        return visitoreeFirstName.compareTo(v.getVisitoreeFirstName());
    }

}
