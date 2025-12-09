package basic;

import advanced.customwritable.ComdValorPaisWritable;
import advanced.customwritable.PaisMediaWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;
import java.io.IOException;

/*6. (Difícil) País com maior preço médio da commodity no fluxo de Exportação;
o Importante: um único país deve ser produzido nesta solução!*/
public class Exercicio6 {
    public static void main(String args[]) throws IOException,
            ClassNotFoundException,
            InterruptedException {
        BasicConfigurator.configure();
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        Path input = new Path(files[0]);
        Path intermediate = new Path("./output/intermediateTDE.tmp");
        Path output = new Path(files[1]);
        Job j1 = new Job(c, "MediaComodite");

        j1.setJarByClass(Exercicio6.class);
        j1.setMapperClass(Exercicio6.MapEtapaA.class);
        j1.setReducerClass(Exercicio6.ReduceEtapaA.class);
        j1.setCombinerClass(CombineEtapaA.class);

        j1.setMapOutputKeyClass(Text.class);
        j1.setMapOutputValueClass(ComdValorPaisWritable.class);
        j1.setOutputKeyClass(Text.class);
        j1.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(j1, input);
        FileOutputFormat.setOutputPath(j1, intermediate);
        j1.waitForCompletion(false);

        Job j2 = new Job(c, "MaiorMediaPais");

        j2.setJarByClass(Exercicio6.class);
        j2.setMapperClass(Exercicio6.MapEtapaB.class);
        j2.setReducerClass(Exercicio6.ReduceEtapaB.class);
        j2.setCombinerClass(CombineEtapaB.class);

        j2.setMapOutputKeyClass(Text.class);
        j2.setMapOutputValueClass(ComdValorPaisWritable.class);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(j2, intermediate);
        FileOutputFormat.setOutputPath(j2, output);
        j2.waitForCompletion(false);
    }

    public static class MapEtapaA extends Mapper<LongWritable, Text, Text, ComdValorPaisWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            String linha = value.toString();
            if (!linha.startsWith("country")) {
                String colunas[] = linha.split(";");
                if (colunas[4].equals("Export")) {
                    String country = colunas[0];
                    float preco = Float.parseFloat(colunas[5]);
                    int qtd = 1;
                    con.write(new Text(country), new ComdValorPaisWritable(preco, qtd));
                }
            }
        }
    }
    public static class CombineEtapaA extends Reducer<Text, ComdValorPaisWritable, Text,
            ComdValorPaisWritable>{
        public void reduce(Text key, Iterable<ComdValorPaisWritable> values, Context con)
                throws IOException, InterruptedException {
            float somaPrecos = 0;
            int somaTotal = 0;
            for(ComdValorPaisWritable o : values){
                somaTotal += o.getQtd();
                somaPrecos += o.getValor();
            }
            con.write(key, new ComdValorPaisWritable(somaPrecos, somaTotal));
        }
    }

    public static class ReduceEtapaA extends Reducer<Text, ComdValorPaisWritable, Text, FloatWritable> {
        public void reduce(Text key, Iterable<ComdValorPaisWritable> values, Context con)
                throws IOException, InterruptedException {
            int somaTotal = 0;
            float somaPrecos = 0;
            for (ComdValorPaisWritable v : values) {
                somaTotal += v.getQtd();
                somaPrecos += v.getValor();
            }
            float media = somaPrecos / somaTotal;
            con.write(key, new FloatWritable(media));
        }
    }

    public static class MapEtapaB extends Mapper<LongWritable, Text, Text, PaisMediaWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            String linha = value.toString();
            String campos [] = linha.split("\t");
            String pais = campos[0];
            float media = Float.parseFloat(campos[1]);
            con.write(new Text("chave"), new PaisMediaWritable(pais, media));
        }
    }

    public static class CombineEtapaB extends Reducer<Text, PaisMediaWritable, Text, PaisMediaWritable>{
        public void reduce(Text key, Iterable<PaisMediaWritable> values, Context con)
                throws IOException, InterruptedException {
            String country = null;
            float valor = Float.MIN_VALUE;
            for(PaisMediaWritable v : values){
                if(v.getValor() > valor){
                    country = v.getPais();
                    valor = v.getValor();
                }
            }
            con.write(key, new PaisMediaWritable(country, valor));
        }
    }

    public static class ReduceEtapaB extends Reducer<Text, PaisMediaWritable, Text, FloatWritable> {
        public void reduce(Text key, Iterable<PaisMediaWritable> values, Context con)
                throws IOException, InterruptedException {
            String pais = null;
            float valor = Float.MIN_VALUE;
            for(PaisMediaWritable v : values){
                if(v.getValor() > valor){
                    pais = v.getPais();
                    valor = v.getValor();
                    System.out.println("pais"+pais);
                    System.out.println("valor"+valor);
                }
            }
            con.write(new Text(pais), new FloatWritable(valor));
        }
    }
}
