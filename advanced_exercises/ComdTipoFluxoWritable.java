package advanced.customwritable;

import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class ComdTipoFluxoWritable
        implements WritableComparable<ComdTipoFluxoWritable> {
    private int comodite;
    private String fluxo;

    public ComdTipoFluxoWritable() {
    }

    public ComdTipoFluxoWritable(int comodite, String fluxo) {
        this.comodite = comodite;
        this.fluxo = fluxo;
    }

    public int getComodite() {
        return comodite;
    }

    public void setComodite(int comodite) {
        this.comodite = comodite;
    }

    public String getFluxo() {
        return fluxo;
    }

    public void setFluxo(String fluxo) {
        this.fluxo = fluxo;
    }

    @Override
    public String toString() {
        return "CommodityTypeFlowWritable{" +
                "commodity='" + comodite + '\'' +
                ", flow=\t" + fluxo +
                "\t}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ComdTipoFluxoWritable that = (ComdTipoFluxoWritable) o;
        return comodite == that.comodite && Objects.equals(fluxo, that.fluxo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(comodite, fluxo);
    }

    @Override
    public int compareTo(ComdTipoFluxoWritable o) {
        return Integer.compare(o.hashCode(), this.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(comodite);
        dataOutput.writeUTF(fluxo);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        comodite = dataInput.readInt();
        fluxo = dataInput.readUTF();
    }
}
