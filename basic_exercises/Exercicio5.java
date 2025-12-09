package basic;

import advanced.customwritable.GroupMaxMinMedia;
import advanced.customwritable.MaxMinMediaWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;
import java.io.IOException;
/*      5. (Médio) O preço de transação máximo, mínimo e médio por tipo de unidade e ano;
*/
public class Exercicio5 {
    public static void main(String args[]) throws IOException,
            ClassNotFoundException,
            InterruptedException {
        BasicConfigurator.configure();
        Configuration c = new Configuration();
        Path input = new Path("in/transactions_amostra.csv");
        Path output = new Path("output/ex5.txt");
        Job j = new Job(c, "MaxMinMedia");

        j.setJarByClass(Exercicio5.class);
        j.setMapperClass(Exercicio5.MapForMinMaxAvg.class);
        j.setReducerClass(Exercicio5.ReduceForMinMaxAvg.class);

        j.setOutputKeyClass(GroupMaxMinMedia.class);
        j.setOutputValueClass(MaxMinMediaWritable.class);
        j.setMapOutputKeyClass(GroupMaxMinMedia.class);
        j.setMapOutputValueClass(MaxMinMediaWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }


    public static class MapForMinMaxAvg extends Mapper<LongWritable, Text, GroupMaxMinMedia, MaxMinMediaWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            String texto = value.toString();
            if(texto.startsWith("country_or_area")){
                return;
            }
            String [] linhas = texto.split("\t");
            String [] valores = linhas[0].split(";");
            String year = valores[1];
            String unitType = valores[7];
            float soma = Float.parseFloat(valores[5]);
            GroupMaxMinMedia groupby = new GroupMaxMinMedia(year, unitType);
            MaxMinMediaWritable valor = new MaxMinMediaWritable(1, soma);
            con.write(groupby, valor);
        }
    }

    public static class ReduceForMinMaxAvg  extends Reducer<GroupMaxMinMedia, MaxMinMediaWritable, GroupMaxMinMedia, MaxMinMediaWritable> {
        private int contador = 0;
        public void reduce(GroupMaxMinMedia groupby, Iterable<MaxMinMediaWritable> values, Context con)
                throws IOException, InterruptedException {
            float somaValores = 0;
            int somaN = 0;
            float precoMax = 0;
            float precoMin = 0;
            for (MaxMinMediaWritable val : values) {
                somaN += val.getN();
                somaValores += val.getPrice();
                float preco =  val.getPrice();
                if (preco > precoMax) {
                    precoMax = preco;
                }
                if (preco < precoMin || precoMin == 0){
                    precoMin = preco;
                }
            }
            float media = somaValores/somaN;
            if (contador < 5) {
                con.write(groupby, new MaxMinMediaWritable(media, precoMax, precoMin));
                contador++;
            }
        }
    }
}