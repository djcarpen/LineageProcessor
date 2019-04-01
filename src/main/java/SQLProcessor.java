
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Stack;
import java.util.TreeSet;

import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * This class prints out the lineage info. It takes sql as input and prints
 * lineage info. Currently this prints only input and output tables for a given
 * sql. Later we can expand to add join tables etc.
 */
public class SQLProcessor implements NodeProcessor {


    TreeSet<String> CTEList = new TreeSet<String>();

    TreeSet<String> inputTableList = new TreeSet<String>();

    TreeSet<String> OutputTableList = new TreeSet<String>();



    public TreeSet<String> getInputTableList() {
        return inputTableList;
    }

    public TreeSet<String> getOutputTableList() {
        return OutputTableList;
    }



    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
                          Object... nodeOutputs) throws SemanticException {
        ASTNode pt = (ASTNode) nd;
        System.out.println(pt.toStringTree());
        //System.out.println("\t\t************** Tree Start *******************\n\t\t"+pt.toStringTree()+"\n\t\t************** Tree End *******************\n");
        switch (pt.getToken().getType()) {
            case HiveParser.TOK_CTE:
                for (int i = 0; i < pt.getChildCount(); i++) {
                    ASTNode temp = (ASTNode) pt.getChild(i);
                    String cteName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) temp.getChild(1));
                    System.out.println("TOK_CTE");
                    System.out.println("\t\t\tNptname: "+getName(pt)+" cteName: "+cteName+" ");
                    CTEList.add(cteName);
                    //inputTableList.remove(cteName);
                    inputTableList.add(cteName);
                    //OutputTableList.add(cteName);
                }
                break;

            case HiveParser.TOK_CREATETABLE:
                //System.out.println("TOK_CREATETABLE");
                //System.out.println("\t\t\tNptName:"+getName(pt)+" ");
                OutputTableList.add(getName(pt));
                break;

            case HiveParser.TOK_TAB:
                //System.out.println("TOK_TAB");
                //System.out.println("\t\t\tNptName:" + getName(pt)+" ");
                OutputTableList.add(getName(pt));
                break;

            case HiveParser.TOK_TABREF:
                ASTNode tabTree = (ASTNode) pt.getChild(0);
                String table_name = (tabTree.getChildCount() == 1) ?
                        getName(tabTree) :
                        getName(tabTree) + "." + tabTree.getChild(1);
                //System.out.println("TOK_TABREF ["+tabTree.getChildCount()+" nodes]");
                for (Node a:tabTree.getChildren()){
                    System.out.println("\t\t\ttable_name: "+getName(tabTree) + "\n\t\t\tchild name: "+a+ "\n");
                }

                //if (table_name.contains("hz_cust_accounts")) System.out.println("ptName:" + getName(pt)+" table_name: "+table_name+ " TOK_TABREF");
                inputTableList.add(table_name);
                break;

        }

        for (String cte : CTEList) {
            if (inputTableList.contains(cte)) {
                //System.out.println("removing " + cte + " from inputTableList");
                inputTableList.remove(cte);
            }
        }

        return null;
    }

    private String getName(ASTNode pt) {
        return BaseSemanticAnalyzer.getUnescapedName((ASTNode) pt.getChild(0));
    }

    public void getLineageInfo(String query) throws ParseException, SemanticException {
        ParseDriver pd = new ParseDriver();
        ASTNode tree = pd.parse(query);


        while ((tree.getToken() == null) && (tree.getChildCount() > 0)) {
            tree = (ASTNode) tree.getChild(0);
        }

        inputTableList.clear();
        OutputTableList.clear();

        Map<Rule, NodeProcessor> rules = new LinkedHashMap<Rule, NodeProcessor>();

        Dispatcher disp = new DefaultRuleDispatcher(this, rules, null);
        GraphWalker ogw = new DefaultGraphWalker(disp);

        ArrayList<Node> topNodes = new ArrayList<Node>();
        topNodes.add(tree);
        ogw.startWalking(topNodes, null);
    }
}
