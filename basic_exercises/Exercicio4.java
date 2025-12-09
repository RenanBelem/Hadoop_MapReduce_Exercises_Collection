package basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;
import advanced.customwritable.TipoUnidadeWritable;
import advanced.customwritable.GroupTipoUnidadeWritable;
import java.io.IOException;
import java.util.Objects;
/*      4. (Fácil) O preço médio das commodities por tipo de unidade, ano e categoria no fluxo de exportação
        no brasil;
*/
public class Exercicio4 {
    public static void main(String args[]) throws IOException,
            ClassNotFoundException,
            InterruptedException {
        BasicConfigurator.configure();
        Configuration c = new Configuration();
        Path input = new Path("in/transactions_amostra.csv");
        Path output = new Path("output/ex4.txt");
        Job j = new Job(c, "MediaPrecoComoditesTipoAnoCategoria");

        j.setJarByClass(TipoUnidadeWritable.class);
        j.setMapperClass(MapForYearUnitCommodityCategory.class);
        j.setReducerClass(ReduceForYearUnitCommodityCategory.class);

        j.setOutputKeyClass(GroupTipoUnidadeWritable.class);
        j.setOutputValueClass(TipoUnidadeWritable.class);
        j.setMapOutputKeyClass(GroupTipoUnidadeWritable.class);
        j.setMapOutputValueClass(TipoUnidadeWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapForYearUnitCommodityCategory extends Mapper<LongWritable, Text, GroupTipoUnidadeWritable, TipoUnidadeWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            String texto = value.toString();
            if(texto.startsWith("country_or_area")){
                return;
            }
            String valorCelula[]=value.toString().split(";");
            String [] linhas = texto.split("\t");
            String [] valores = linhas[0].split(";");
            String ano = valores[1];
            String comdNome = valores[2];
            String tipoUnid = valores[7];
            String categoria = valores[8];
            String fluxo = valores[4];
            String pais = valores[0];
            float soma = Float.parseFloat(valores[5]);
            if ((Objects.equals(pais, "Brazil")) && (Objects.equals(fluxo, "Export"))){
                GroupTipoUnidadeWritable groupby = new GroupTipoUnidadeWritable(ano, comdNome, tipoUnid, categoria);
                TipoUnidadeWritable val = new TipoUnidadeWritable(1, soma);
                con.write(groupby, val);
            }
        }
    }

    public static class ReduceForYearUnitCommodityCategory extends Reducer<GroupTipoUnidadeWritable,
            TipoUnidadeWritable, GroupTipoUnidadeWritable, FloatWritable> {
        private int contador = 0;
        public void reduce(GroupTipoUnidadeWritable groupby, Iterable<TipoUnidadeWritable> values, Context con)
                throws IOException, InterruptedException {
            float somaValores = 0;
            int somaN = 0;
            for (TipoUnidadeWritable val : values) {
                somaN += val.getN();
                somaValores += val.getPreco();
            }
            float media = somaValores/somaN;
            if(contador < 5) {
                con.write(groupby, new FloatWritable(media));
                contador++;
            }
        }
    }
}
