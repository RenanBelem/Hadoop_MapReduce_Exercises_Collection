package advanced.customwritable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class ComparaComdWritable
        implements WritableComparable<ComparaComdWritable> {
    private String comodite;
    private int qtd;
    private String fluxo;

    public ComparaComdWritable() {
    }

    public ComparaComdWritable(String comodite, int qtd, String fluxo) {
        this.comodite = comodite;
        this.qtd = qtd;
        this.fluxo = fluxo;
    }

    public String getComodite() {
        return comodite;
    }

    public void setComodite(String comodite) {
        this.comodite = comodite;
    }

    public int getQtd() {
        return qtd;
    }

    public void setQtd(int qtd) {
        this.qtd = qtd;
    }

    public String getFluxo() {
        return fluxo;
    }

    public void setFluxo(String fluxo) {
        this.fluxo = fluxo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ComparaComdWritable that = (ComparaComdWritable) o;
        return qtd == that.qtd && Objects.equals(comodite, that.comodite) && Objects.equals(fluxo, that.fluxo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(comodite, qtd, fluxo);
    }

    @Override
    public int compareTo(ComparaComdWritable o) {
        return Integer.compare(o.hashCode(), this.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(comodite);
        dataOutput.writeInt(qtd);
        dataOutput.writeUTF(fluxo);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        comodite = dataInput.readUTF();
        qtd = dataInput.readInt();
        fluxo = dataInput.readUTF();
    }
}
