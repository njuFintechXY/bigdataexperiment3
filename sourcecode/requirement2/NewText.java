package example;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class NewText implements WritableComparable<NewText>{
	private String a;
	private int fre;
	
	public NewText() {}
	public void set(String line) {
		String[] linesplit = line.split(" ");
		a = linesplit[0];
		fre = Integer.parseInt(linesplit[1]);
	}
	public String geta() {return a;}
	public int getfre() {return fre;}
	public String toString() {
		return a+" "+Integer.toString(fre);
	}
	public void readFields(DataInput in) throws IOException{
		a = in.readUTF();
		fre = in.readInt();
	}
	
	public void write(DataOutput out) throws IOException{
		out.writeUTF(a);
		out.writeInt(fre);
	}
	
	public int compareTo(NewText o) {
		int wordcompare = a.compareTo(o.geta());
		int frediss = fre-o.getfre();
		if(wordcompare < 0)return 1;
		else if(wordcompare > 0)return -1;
		else return -frediss;
	}
}
