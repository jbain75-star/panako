package be.panako.strategy.panako;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.charset.StandardCharsets;
import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

/** Offline query checks using the working strategy's actual matching methods. */
public class QueryCleanupCheck {
    public static void main(String[] args) throws Exception {
        Path root = Paths.get("").toAbsolutePath();
        Path resources = root.resolve("src/test/resources/query-cleanup");
        Path output = root.resolve("build/query-cleanup-check");
        Files.createDirectories(output);
        String strategy = Files.readString(root.resolve("src/main/java/be/panako/strategy/panako/PanakoStrategy.java"));
        int helpersStart = strategy.indexOf("\tprivate float blocksToSeconds");
        int helpersEnd = strategy.indexOf("\t@Override\n\tpublic void query", helpersStart);
        int queryStart = strategy.indexOf("\tprivate void query(String query,");
        int queryEnd = strategy.indexOf("\n\t@Override", queryStart);
        if(helpersStart < 0 || helpersEnd < 0 || queryStart < 0 || queryEnd < 0) {
            throw new AssertionError("Cannot locate production query methods");
        }
        String actual = strategy.substring(helpersStart, helpersEnd)
                + strategy.substring(queryStart, queryEnd).replace("private void query(", "public void query(");
        String baseline = Files.readString(resources.resolve("baseline.java.txt"));
        String replay = Files.readString(resources.resolve("Replay.java.template"))
                .replace("/*BASELINE*/", variant("baseline", baseline))
                .replace("/*OPTIMIZED*/", variant("optimized", actual));
        Path replayFile = output.resolve("QueryReplay.java");
        Path goldenFile = output.resolve("QueryGolden.java");
        Files.writeString(replayFile, replay, StandardCharsets.UTF_8);
        Files.writeString(goldenFile, Files.readString(resources.resolve("Golden.java.template")), StandardCharsets.UTF_8);
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        if(compiler == null || compiler.run(null, null, null, "--release", "11", "-d", output.toString(),
                replayFile.toString(), goldenFile.toString()) != 0) {
            throw new AssertionError("Could not compile actual query replay");
        }
        if(args.length == 0) {
            execute(output, "QueryGolden");
            execute(output, "QueryReplay", "verify");
        }else{
            String[] command = new String[args.length + 1];
            command[0] = "QueryReplay";
            System.arraycopy(args, 0, command, 1, args.length);
            execute(output, command);
        }
    }

    private static String variant(String name, String methods) {
        return "static class " + name + " extends Base { " + name + "(Fixture f){super(f);}\n" + methods + "\n}\n";
    }

    private static void execute(Path output, String... arguments) throws Exception {
        String[] command = new String[arguments.length + 4];
        command[0] = Paths.get(System.getProperty("java.home"), "bin", "java").toString();
        command[1] = "-Xmx2048m";
        command[2] = "-cp";
        command[3] = output.toString();
        System.arraycopy(arguments, 0, command, 4, arguments.length);
        if(new ProcessBuilder(command).inheritIO().start().waitFor() != 0) {
            throw new AssertionError("Query replay failed");
        }
    }
}
