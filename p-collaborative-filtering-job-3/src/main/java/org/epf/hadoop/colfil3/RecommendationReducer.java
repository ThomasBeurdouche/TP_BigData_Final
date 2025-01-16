package org.epf.hadoop.colfil3;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.*;

public class RecommendationReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String, Integer> candidateMap = new HashMap<>();

        for (Text value : values) {
            String[] parts = value.toString().split(",");
            String candidate = parts[0];
            int commonCount = Integer.parseInt(parts[1]);
            candidateMap.put(candidate, candidateMap.getOrDefault(candidate, 0) + commonCount);
        }

        List<Map.Entry<String, Integer>> sortedCandidates = new ArrayList<>(candidateMap.entrySet());
        sortedCandidates.sort((e1, e2) -> {
            int cmp = e2.getValue().compareTo(e1.getValue());
            if (cmp != 0) return cmp;
            return e1.getKey().compareTo(e2.getKey());
        });

        List<String> topRecommendations = new ArrayList<>();
        for (int i = 0; i < Math.min(5, sortedCandidates.size()); i++) {
            Map.Entry<String, Integer> entry = sortedCandidates.get(i);
            topRecommendations.add(entry.getKey() + " (" + entry.getValue() + ")");
        }

        context.write(key, new Text(String.join(", ", topRecommendations)));
    }
}
