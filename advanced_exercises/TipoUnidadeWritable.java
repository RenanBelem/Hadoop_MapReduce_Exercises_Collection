package advanced.customwritable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TipoUnidadeWritable implements Writable {
    private int n;
    private float preco;

    public TipoUnidadeWritable() {
    }

    public TipoUnidadeWritable(int n, float preco) {
        this.n = n;
        this.preco = preco;
    }

    public int getN() {
        return n;
    }

    public void setN(int n) {
        this.n = n;
    }

    public float getPreco() {
        return preco;
    }

    public void setPreco(float preco) {
        this.preco = preco;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(n);
        dataOutput.writeFloat(preco);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        n = dataInput.readInt();
        preco = dataInput.readFloat();
    }







}