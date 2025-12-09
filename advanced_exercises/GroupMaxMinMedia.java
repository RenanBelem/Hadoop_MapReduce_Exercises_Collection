package advanced.customwritable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class GroupMaxMinMedia implements WritableComparable<GroupMaxMinMedia> {
    public String ano;
    public String tipoUnid;

    public GroupMaxMinMedia() {

    }

    public GroupMaxMinMedia(String ano, String tipoUnid) {
        this.ano = ano;
        this.tipoUnid = tipoUnid;

    }

    @Override
    public int compareTo(GroupMaxMinMedia o) {
        if (o == null) {
            return 0;
        }
        int ano = this.ano.compareTo(o.ano);

        return ano == 0 ? tipoUnid.compareTo(o.tipoUnid) : ano;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(ano);
        dataOutput.writeUTF(tipoUnid);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        ano = dataInput.readUTF();
        tipoUnid = dataInput.readUTF();
    }
    @Override
    public String toString() {
        return this.ano + "\t" + this.tipoUnid + "\t" ;
    }

}