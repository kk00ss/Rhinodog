package test

/**
  * Created by kos on 07-Apr-16.
  */
package object scala {
    def cleanup(testDir: java.io.File)  = {
        for (subDir <- testDir.listFiles()) {
            for (file <- subDir.listFiles())
                file.delete()
            subDir.delete()
        }
        testDir.delete()
    }
}
