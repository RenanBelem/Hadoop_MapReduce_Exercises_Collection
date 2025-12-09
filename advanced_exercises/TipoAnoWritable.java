package advanced.customwritable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class TipoAnoWritable implements WritableComparable {
    // Some data
    private String ano;
    private String fluxo;

    //    @Override
    public void readFields(DataInput in) throws IOException {
        ano = in.readUTF();
        fluxo = in.readUTF();
    }

    //    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(ano);
        out.writeUTF(fluxo);
    }

    @Override
    public int compareTo(Object o) {
        if ( o instanceof TipoAnoWritable) {
            TipoAnoWritable cy = (TipoAnoWritable) o;
            return ( this.fluxo.compareTo(cy.fluxo) + this.ano.compareTo(cy.ano) );
        }
        return -1;
    }

    @Override
    public String toString() {
        return this.ano + "\t" + this.fluxo;
    }

    public String getAno() { return ano; }

    public void setAno(String ano) { this.ano = ano; }

    public String getFluxo() { return fluxo; }

    public void setFluxo(String fluxo) { this.fluxo = fluxo; }

    public TipoAnoWritable() {
    }

    public TipoAnoWritable(String ano, String fluxo) {
        this.ano = ano;
        this.fluxo = fluxo;
    }

}