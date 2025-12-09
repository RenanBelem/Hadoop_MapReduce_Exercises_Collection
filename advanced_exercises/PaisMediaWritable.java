package advanced.customwritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class PaisMediaWritable
        implements WritableComparable<PaisMediaWritable> {
    private String pais;
    private float valor;

    public PaisMediaWritable() {
    }

    public PaisMediaWritable(String pais, float valot) {
        this.pais = pais;
        this.valor = valot;
    }

    public String getPais() {
        return pais;
    }

    public void setPais(String pais) {
        this.pais = pais;
    }

    public float getValor() {
        return valor;
    }

    public void setValor(float valor) {
        this.valor = valor;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PaisMediaWritable that = (PaisMediaWritable) o;
        return Float.compare(that.valor, valor) == 0 && Objects.equals(pais, that.pais);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pais, valor);
    }

    @Override
    public int compareTo(PaisMediaWritable o) {
        return Integer.compare(o.hashCode(), this.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(pais);
        dataOutput.writeFloat(valor);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        pais = dataInput.readUTF();
        valor = dataInput.readFloat();
    }
}
