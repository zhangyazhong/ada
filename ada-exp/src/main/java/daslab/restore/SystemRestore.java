package daslab.restore;

import com.google.common.collect.ImmutableSet;

import java.util.Set;

public class SystemRestore {
    public static Set<RestoreModule> restoreModules() {
        return ImmutableSet.of(
                new DatabaseRestore(),
                new ProducerRestore(),
                new SampleCleaner()
        );
    }
}
