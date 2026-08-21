package gkza.flink.demo4;

import gkza.flink.utils.DateUtil;
import org.apache.flink.core.fs.Path;

import java.io.Serializable;
import java.util.function.Predicate;

/**
 * @Author: MaoMao
 * @Date: 2026/8/21 16:55
 * @description:
 */
public class MyPathFilter implements Predicate<Path>, Serializable {

    @Override
    public boolean test(Path path) {
        String fileName = path.toString();

        String toDayDate = DateUtil.getToDayDate();

        return !fileName.contains(toDayDate);
    }
}
