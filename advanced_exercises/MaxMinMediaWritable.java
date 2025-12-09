package advanced.customwritable;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MaxMinMediaWritable implements Writable {
    private int n;
    private float preco;
    private float maxPreco;
    private float minPreco;
    public MaxMinMediaWritable() {
    }

    public MaxMinMediaWritable(int n, float preco, float maxPreco, float minPreco) {
        this.n = n;
        this.preco = preco;
        this.maxPreco = maxPreco;
        this.minPreco = minPreco;
    }

    public MaxMinMediaWritable(float preco, float maxPreco, float minPreco) {
        this.preco = preco;
        this.maxPreco = maxPreco;
        this.minPreco = minPreco;
    }

    public MaxMinMediaWritable(int n, float preco) {
        this.n = n;
        this.preco = preco;
    }

    public int getN() {
        return n;
    }

    public void setN(int n) {
        this.n = n;
    }

    public float getPrice() {
        return preco;
    }

    public void setPrice(float preco) {
        this.preco = preco;
    }

    public float getmaxPreco() {
        return maxPreco;
    }

    public void setmaxPreco(float maxPreco) {
        this.maxPreco = maxPreco;
    }

    public float getminPreco() {
        return minPreco;
    }

    public void setminPreco(float minPreco) {
        this.minPreco = minPreco;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(n);
        dataOutput.writeFloat(preco);
        dataOutput.writeFloat(maxPreco);
        dataOutput.writeFloat(minPreco);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        n = dataInput.readInt();
        preco = dataInput.readFloat();
        maxPreco = dataInput.readFloat();
        minPreco = dataInput.readFloat();
    }
    @Override
    public String toString() {
        return this.preco + "\t" + this.maxPreco + "\t" + this.minPreco + "\t" ;
    }

}