awk '/ Q / && / R / { start[$3] = $4} 
    / C / && / R / && start[$3] {
        sectorCount = $10; # Number of sectors
        duration = $4 - start[$3];
        print "start: " start[$3]
        print "end: " $4
        print "duration " duration

        if(sectorCount <= 6) { # <= 24KB
            count_small++;
            total_small += duration;
        } else { # > 24KB
            count_large++;
            total_large += duration;
        }
        delete start[$3];
    }
    END {
        if(count_small > 0) {
            print "Average r_await for <= 24KB reads: " total_small / count_small " s";
        }
        if(count_large > 0) {
            print "Total time: " total_large
            print "Total reads: " count_large
            print "Average r_await for > 24KB reads: " total_large / count_large " s";
        }
    }
' blktrace_output.txt

awk '
    / Q / && / R / { start[$5 "_" $3] = $4}
    / C / && / R / && start[$5 "_" $3] {
        sectorCount = $10; # Number of sectors
        duration = $4 - start[$5 "_" $3];
        print "Start: " start[$5 "_" $3];
        print "End: " $4;
        print "Duration: " duration " s";

        if(sectorCount <= 6) { # <= 24KB
            count_small++;
            total_small += duration;
        } else { # > 24KB
            count_large++;
            total_large += duration;
        }
        delete start[$5 "_" $3];
    }
    END {
        if(count_small > 0) {
            print "Average r_await for <= 24KB reads: ", total_small / count_small " s";
        }
        if(count_large > 0) {
            print "Total time for > 24KB reads: ", total_large " s";
            print "Total > 24KB reads: ", count_large;
            print "Average r_await for > 24KB reads: ", total_large / count_large " s";
        }
    }
' blktrace_output.txt