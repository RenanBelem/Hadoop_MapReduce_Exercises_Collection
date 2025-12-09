package advanced.customwritable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class GroupTipoUnidadeWritable implements WritableComparable<GroupTipoUnidadeWritable> {
    public String ano;
    public String nomeComd;
    public String tipoUnid;
    public String categoria;

    public GroupTipoUnidadeWritable() {

    }

    public GroupTipoUnidadeWritable(String ano, String nomeComd, String tipoUnid, String categoria) {
        this.ano = ano;
        this.nomeComd = nomeComd;
        this.tipoUnid = tipoUnid;
        this.categoria = categoria;
    }

    @Override
    public int compareTo(GroupTipoUnidadeWritable o) {
        if (o == null) {
            return 0;
        }
        int ano = this.ano.compareTo(o.ano);
        if (ano != 0){
            return ano;
        }
        int nome = nomeComd.compareTo(o.nomeComd);
        if(nome!= 0){
            return nome;
        }
        int tipoUnidade = tipoUnid.compareTo(o.tipoUnid);
        if(tipoUnidade != 0) {
            return tipoUnidade;
        }
        int categoria = this.categoria.compareTo(o.categoria);
        return categoria;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(ano);
        dataOutput.writeUTF(nomeComd);
        dataOutput.writeUTF(tipoUnid);
        dataOutput.writeUTF(categoria);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        ano = dataInput.readUTF();
        nomeComd = dataInput.readUTF();
        tipoUnid = dataInput.readUTF();
        categoria = dataInput.readUTF();
    }
    @Override
    public String toString() {
        return this.nomeComd + "\t" + this.ano + "\t" + this.tipoUnid + "\t" + this.categoria + "\t" ;
    }


}
