package basic;

import advanced.customwritable.ComdTipoFluxoWritable;
import advanced.customwritable.ComparaComdWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;
import java.io.IOException;

/*7. (Difícil) Commodity mais comercializada (somando as quantidades) em 2016, por fluxo tipo*/
public class Exercicio7 {
    public static void main(String args[]) throws IOException,
            ClassNotFoundException,
            InterruptedException{
        BasicConfigurator.configure();
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        Path input = new Path(files[0]);
        Path intermediate = new Path("./output/intermediateComoditeTDE.tmp");
        Path output = new Path(files[1]);
        Job j1 = new Job(c, "ComoditeMaisComercializada");

        j1.setJarByClass(Exercicio7.class);
        j1.setMapperClass(MapEtapaA.class);
        j1.setCombinerClass(CombineEtapaA.class);
        j1.setReducerClass(ReduceEtapaA.class);

        j1.setMapOutputKeyClass(ComdTipoFluxoWritable.class);
        j1.setMapOutputValueClass(IntWritable.class);
        j1.setOutputKeyClass(ComdTipoFluxoWritable.class);
        j1.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(j1, input);
        FileOutputFormat.setOutputPath(j1, intermediate);
        j1.waitForCompletion(false);

        Job j2 = new Job(c, "ComparaComodite");

        j2.setJarByClass(Exercicio7.class);
        j2.setMapperClass(MapEtapaB.class);
        j2.setCombinerClass(CombineEtapaB.class);
        j2.setReducerClass(ReduceEtapaB.class);

        j2.setMapOutputKeyClass(Text.class);
        j2.setMapOutputValueClass(ComdTipoFluxoWritable.class);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(j2, intermediate);
        FileOutputFormat.setOutputPath(j2, output);
        j2.waitForCompletion(false);
    }
    public static class MapEtapaA extends Mapper<LongWritable, Text, ComdTipoFluxoWritable, IntWritable> {  // Setar valores de entrada e saida
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException{
            String linha = value.toString();
            if (!linha.startsWith("country")){
                String colunas[] = linha.split(";");
                if(colunas[1].equals("2016")) {
                    int comodite = Integer.parseInt(colunas[2]);
                    String fluxo = colunas[4];
                    int qtd = 1;
                    con.write(new ComdTipoFluxoWritable(comodite, fluxo), new IntWritable(qtd));
                }
            }
        }
    }

    public static class CombineEtapaA extends Reducer<ComdTipoFluxoWritable, IntWritable,
            ComdTipoFluxoWritable, IntWritable> {
        public void reduce(ComdTipoFluxoWritable key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {
            int somaQtds = 0;
            for (IntWritable v : values){
                somaQtds += v.get();
            }
            con.write(key, new IntWritable(somaQtds));
        }
    }

    public  static class ReduceEtapaA extends Reducer<ComdTipoFluxoWritable, IntWritable,
            ComdTipoFluxoWritable, IntWritable>{
        public void reduce(ComdTipoFluxoWritable key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException{
            int somaQtds = 0;
            for (IntWritable v : values){
                somaQtds += v.get();
            }
            con.write(key, new IntWritable(somaQtds));
        }
    }

    public static class MapEtapaB extends Mapper<LongWritable, Text, Text,
            ComparaComdWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException{
            String linha = value.toString();
            String colunas[] = linha.split("\t");
            String comodite = colunas[0];
            String fluxo = colunas[1];
            int qtd = Integer.parseInt(colunas[3]);
            con.write(new Text("chave"), new ComparaComdWritable(comodite, qtd, fluxo));
        }
    }

    public static class CombineEtapaB extends Reducer<Text, ComparaComdWritable, Text,
            ComparaComdWritable> {
        public void reduce(Text key, Iterable<ComparaComdWritable> values, Context con)
                throws IOException, InterruptedException {
            int maiorSomaExp = Integer.MIN_VALUE;
            ComparaComdWritable comoditeExp = new ComparaComdWritable();
            int maiorSomaImp = Integer.MIN_VALUE;
            ComparaComdWritable comoditeImport = new ComparaComdWritable();
            for (ComparaComdWritable v : values){
                if(v.getFluxo().equals("Export")){
                    if(v.getQtd() > maiorSomaExp){
                        maiorSomaExp = v.getQtd();
                        comoditeExp.setComodite(v.getComodite());
                        comoditeExp.setQtd(v.getQtd());
                        comoditeExp.setFluxo(v.getFluxo());
                    }
                }
                if(v.getFluxo().equals("Import")){
                    if(v.getQtd() > maiorSomaImp){
                        maiorSomaImp = v.getQtd();
                        comoditeImport.setFluxo(v.getFluxo());
                        comoditeImport.setQtd(v.getQtd());
                        comoditeImport.setComodite(v.getComodite());
                    }
                }
            }
            con.write(new Text("Export"), comoditeExp);
            con.write(new Text("Import"), comoditeImport);
        }
    }

    public  static class ReduceEtapaB extends Reducer<Text, ComparaComdWritable, Text, IntWritable>{
        public void reduce(Text key, Iterable<ComparaComdWritable> values, Context con)
                throws IOException, InterruptedException{
            int maiorSoma = Integer.MIN_VALUE;
            ComparaComdWritable comodite = new ComparaComdWritable();
            for (ComparaComdWritable v : values){
                if(v.getQtd() > maiorSoma){
                    maiorSoma = v.getQtd();
                    comodite.setQtd(v.getQtd());
                    comodite.setFluxo(v.getFluxo());
                    comodite.setComodite(v.getComodite());
                }
            }
            con.write(new Text(comodite.getComodite() + comodite.getFluxo()), new IntWritable(maiorSoma));
        }
    }
}