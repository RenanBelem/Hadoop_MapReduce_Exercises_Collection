package advanced.customwritable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class ComdValorPaisWritable
        implements WritableComparable<ComdValorPaisWritable> {
    private float valor;
    private int qtd;

    public ComdValorPaisWritable() {
    }

    public ComdValorPaisWritable(float valor, int qtd) {
        this.valor = valor;
        this.qtd = qtd;
    }

    public float getValor() {
        return valor;
    }

    public void setValor(float valor) {
        this.valor = valor;
    }

    public int getQtd() {
        return qtd;
    }

    public void setQtd(int qtd) {
        this.qtd = qtd;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ComdValorPaisWritable that = (ComdValorPaisWritable) o;
        return Float.compare(that.valor, valor) == 0 && qtd == that.qtd;
    }

    @Override
    public int hashCode() {
        return Objects.hash(valor, qtd);
    }

    @Override
    public int compareTo(ComdValorPaisWritable o) {
        return Integer.compare(o.hashCode(), this.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(valor);
        dataOutput.writeInt(qtd);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        valor = dataInput.readFloat();
        qtd = dataInput.readInt();
    }
}
