package advanced.customwritable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class ComdAnoValorWritable implements WritableComparable {
    private String comodite;
    private String ano;

    //    @Override
    public void readFields(DataInput in) throws IOException {
        comodite = in.readUTF();
        ano = in.readUTF();
    }

    //    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(comodite);
        out.writeUTF(ano);
    }

    @Override
    public int compareTo(Object o) {
        if ( o instanceof ComdAnoValorWritable) {
            ComdAnoValorWritable cy = (ComdAnoValorWritable) o;
            return ( this.comodite.compareTo(cy.comodite) + this.ano.compareTo(cy.ano) );
        }
        return -1;
    }

    @Override
    public boolean equals(Object o) {
        return ( this.compareTo(o) == 0 );
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.comodite, this.ano);
    }

    @Override
    public String toString() {
        return this.comodite + "\t" + this.ano;
    }

    public String getcomodite() { return comodite; }

    public void setcomodite(String comodite) { this.comodite = comodite; }

    public String getano() { return ano; }

    public void setano(String flow) { this.ano = ano; }

    public ComdAnoValorWritable() {
    }

    public ComdAnoValorWritable(String comodite, String ano) {
        this.comodite = comodite;
        this.ano = ano;
    }

}