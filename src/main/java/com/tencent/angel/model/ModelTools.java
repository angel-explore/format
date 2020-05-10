/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.model;

import com.tencent.angel.ml.math2.matrix.Matrix;
import com.tencent.angel.model.io.MatrixLoaderSaver;
import com.tencent.angel.model.output.format.Format;
import com.tencent.angel.model.output.format.MatrixFilesMeta;
import com.tencent.angel.model.output.format.ModelFilesConstent;
import com.tencent.angel.model.output.format.ModelFilesUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 模型本地加载工具
 */
public class ModelTools {


    /**
     * 将模型加载到本地进程内存
     *
     * @param loadContext 模型加载上线文
     * @param conf        配置文件
     * @return ModelLocalLoadResult
     * @throws IOException
     */
    public static ModelLocalLoadResult loadToLocal(ModelLoadContext loadContext, Configuration conf) throws IOException {
        List<MatrixLoadContext> matrixLoadContexts = loadContext.getMatricesContext();//根据模型获取矩阵上下文
        Map<String, Matrix> nameToMatrixMap = new HashMap<>(matrixLoadContexts.size());
        for (MatrixLoadContext matrixLoadContext : matrixLoadContexts) {
            if (matrixLoadContext.getLoadPath() == null) {
                matrixLoadContext.setLoadPath(new Path(loadContext.getLoadPath(), matrixLoadContext.getMatrixName()).toString());
            }
            nameToMatrixMap.put(matrixLoadContext.getMatrixName(), loadToLocal(matrixLoadContext, conf));
        }

        return new ModelLocalLoadResult(nameToMatrixMap);
    }

    /**
     *
     * @param loadContext
     * @param conf
     * @return
     * @throws IOException
     */
    public static Matrix loadToLocal(MatrixLoadContext loadContext, Configuration conf) throws IOException {
        try {
            // Read matrix meta from meta file
            Path metaFilePath = new Path(loadContext.getLoadPath(), ModelFilesConstent.modelMetaFileName);
            FileSystem fs = metaFilePath.getFileSystem(conf);
            if (!fs.exists(metaFilePath)) {
                throw new IOException(
                        "Can not find meta file for matrix " + loadContext.getMatrixName() + " on path "
                                + loadContext.getLoadPath());
            }
            MatrixFilesMeta matrixFilesMeta;
            fs.setVerifyChecksum(false);
            FSDataInputStream input = fs.open(metaFilePath);
            matrixFilesMeta = new MatrixFilesMeta();
            try {
                matrixFilesMeta.read(input);
            } catch (Throwable e) {
                throw new IOException("Read meta failed ", e);
            } finally {
                input.close();
            }

            Format format = ModelFilesUtils.initFormat(matrixFilesMeta.getFormatClassName(), conf);
            MatrixLoaderSaver loaderSaver = ModelFilesUtils.initLoaderSaver(format, conf);
            return loaderSaver.load(loadContext, conf);
        } catch (Throwable e) {
            throw new IOException(e);
        }
    }

    public static void main(String[] args) throws Exception {
        String LOCAL_FS = FileSystem.DEFAULT_FS;
        String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");
        String savePath = LOCAL_FS + TMP_PATH + "/FMmodel";

        ModelLoadContext loadContext = new ModelLoadContext(savePath);
        loadContext.addMatrix(new MatrixLoadContext("embedding_embedding"));
        loadContext.addMatrix(new MatrixLoadContext("input_bias"));
        loadContext.addMatrix(new MatrixLoadContext("input_weight"));
        ModelLocalLoadResult result = loadToLocal(loadContext, new Configuration());
        result.getNameToMatrixMap();
    }
}
