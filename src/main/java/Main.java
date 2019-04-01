import jodd.io.StreamGobbler;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;

import static java.nio.file.Files.readAllBytes;

public class Main {
    public static void main(String[] args) throws IOException, ParseException, SemanticException {
        Set nodeSet = new HashSet();


        Map<TreeSet<String>, TreeSet<String>> lineageMap = new LinkedHashMap<>();

        String file = "/Users/dc185246/Desktop/parser_test.hql";
        String query = new String(readAllBytes(Paths.get(file)), StandardCharsets.UTF_8);
        String[] sqlStatements = Arrays.stream(query.split(";"))
                .map(String::trim)
                .toArray(String[]::new);
        for (String s : sqlStatements) {
            if (!s.isEmpty()) {
                System.out.println("************** SQL Statement Start *******************\n"+s+"\n************** SQL Statement End *******************\n");
                SQLProcessor lep = new SQLProcessor();
                lep.getLineageInfo(s);

                if (!lep.getOutputTableList().isEmpty()) {
                    lineageMap.put(lep.getOutputTableList(), lep.getInputTableList());
                    for (String outputTable:lep.getOutputTableList()) {
                        if (!nodeSet.contains(outputTable)) nodeSet.add(outputTable);
                    }
                    for (String inputTable:lep.getInputTableList()) {
                        if (!nodeSet.contains(inputTable)) nodeSet.add(inputTable);
                    }
                }

            }
        }
        StringBuilder sb = new StringBuilder();
        sb.append("digraph OrdersRepository {\nnode [shape=box];  ").append("\n");
        nodeSet.forEach(t->sb.append("\""+((String) t).toLowerCase()).append("\""+";"));
        sb.append("\n");
        lineageMap.entrySet().forEach(t -> {
            //System.out.println(t.getKey() + " **** " + t.getValue());
            t.getKey().forEach(k-> {
                //System.out.println("key:  " + k.toLowerCase());
                t.getValue().forEach(v -> sb.append("\""+v.toLowerCase()).append("\""+"->").append("\""+k.toLowerCase()).append("\""+"\n"));

                });
            });




        for (Map.Entry entry : lineageMap.entrySet()) {

            //System.out.println("Key = " + entry.getKey().toString().toLowerCase() + ", Value = " + entry.getValue().toString().toLowerCase());

        }

        sb.append("overlap=false\n" +
                "label=\"Justin Carpenter2 Graphviz Orders Repository\"\n" +
                "fontsize=12;\n" +
                "}");
       // System.out.println("\n\n\n\n"+sb);






        //commands.add("/Users/dc185246/neato -Tpng /Users/dc185246/Desktop/OrdersRepository2.gv > /Users/dc185246/Desktop/OrdersRepository2.png");


        FileWriter fileWriter = new FileWriter("/Users/dc185246/Desktop/OrdersRepository2.gv");
        fileWriter.write(sb.toString());
        fileWriter.close();


        try {
            //Runtime.getRuntime().exec("rm OrdersRepository2.png");
            Runtime.getRuntime().exec("neato -Tpng /Users/dc185246/Desktop/OrdersRepository2.gv > /Users/dc185246/Desktop/OrdersRepository2.png");

            System.exit(0);

        }
        catch (IOException e) {
            System.out.println("exception happened - here's what I know: ");
            e.printStackTrace();
            System.exit(-1);
        }



        /*
        digraph OrdersRepository {
node [shape=box];  fact_order_line; dim_order_header; dim_global_accounting_calendar; hr_all_organization_units; hr_operating_units; oe_order_headers_all; oe_order_lines_all;
oe_order_headers_all->fact_order_line;
oe_order_lines_all->fact_order_line;
hr_all_organization_units->fact_order_line;
hr_operating_units->fact_order_line;
dim_order_header->fact_order_line;
dim_global_accounting_calendar->fact_order_line;
oe_order_headers_all->dim_order_header;
overlap=false
label="Justin Carpenter Graphviz Orders Repository"
fontsize=12;
}

         */


    }
}

