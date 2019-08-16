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
package com.tencent.angel.model.output.serde;

import com.tencent.angel.model.output.element.IntDoublesCol;
import com.tencent.angel.model.output.element.IntFloatsCol;
import com.tencent.angel.model.output.element.IntIntsCol;
import com.tencent.angel.model.output.element.IntLongsCol;
import com.tencent.angel.model.output.element.LongDoublesCol;
import com.tencent.angel.model.output.element.LongFloatsCol;
import com.tencent.angel.model.output.element.LongIntsCol;
import com.tencent.angel.model.output.element.LongLongsCol;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Column first format for matrix
 */
public interface ColumnSerDe {

  /**
   * Write a matrix column to output stream
   *
   * @param col matrix column for int key float value matrix
   * @param output output stream
   */
  void serialize(IntFloatsCol col, DataOutputStream output) throws IOException;

  /**
   * Write a matrix column to output stream
   *
   * @param col matrix column for int key double value matrix
   * @param output output stream
   */
  void serialize(IntDoublesCol col, DataOutputStream output) throws IOException;

  /**
   * Write a matrix column to output stream
   *
   * @param col matrix column for int key int value matrix
   * @param output output stream
   */
  void serialize(IntIntsCol col, DataOutputStream output) throws IOException;

  /**
   * Write a matrix column to output stream
   *
   * @param col matrix column for int key long value matrix
   * @param output output stream
   */
  void serialize(IntLongsCol col, DataOutputStream output) throws IOException;

  /**
   * Write a matrix column to output stream
   *
   * @param col matrix column for long key float value matrix
   * @param output output stream
   */
  void serialize(LongFloatsCol col, DataOutputStream output) throws IOException;

  /**
   * Write a matrix column to output stream
   *
   * @param col matrix column for long key float double matrix
   * @param output output stream
   */
  void serialize(LongDoublesCol col, DataOutputStream output) throws IOException;

  /**
   * Write a matrix column to output stream
   *
   * @param col matrix column for long key int value matrix
   * @param output output stream
   */
  void serialize(LongIntsCol col, DataOutputStream output) throws IOException;

  /**
   * Write a matrix column to output stream
   *
   * @param col matrix column for long key long value matrix
   * @param output output stream
   */
  void serialize(LongLongsCol col, DataOutputStream output) throws IOException;

  /**
   * Read a matrix column to input stream
   *
   * @param col matrix column for int key float value matrix
   * @param output input stream
   */
  void deserialize(IntFloatsCol col, DataInputStream output) throws IOException;

  /**
   * Read a matrix column to input stream
   *
   * @param col matrix column for int key double value matrix
   * @param output input stream
   */
  void deserialize(IntDoublesCol col, DataInputStream output) throws IOException;

  /**
   * Read a matrix column to input stream
   *
   * @param col matrix column for int key int value matrix
   * @param output input stream
   */
  void deserialize(IntIntsCol col, DataInputStream output) throws IOException;

  /**
   * Read a matrix column to input stream
   *
   * @param col matrix column for int key long value matrix
   * @param output input stream
   */
  void deserialize(IntLongsCol col, DataInputStream output) throws IOException;

  /**
   * Read a matrix column to input stream
   *
   * @param col matrix column for long key float value matrix
   * @param output input stream
   */
  void deserialize(LongFloatsCol col, DataInputStream output) throws IOException;

  /**
   * Read a matrix column to input stream
   *
   * @param col matrix column for long key float value matrix
   * @param output input stream
   */
  void deserialize(LongDoublesCol col, DataInputStream output) throws IOException;

  /**
   * Read a matrix column to input stream
   *
   * @param col matrix column for long key int value matrix
   * @param output input stream
   */
  void deserialize(LongIntsCol col, DataInputStream output) throws IOException;

  /**
   * Read a matrix column to input stream
   *
   * @param col matrix column for long key long value matrix
   * @param output input stream
   */
  void deserialize(LongLongsCol col, DataInputStream output) throws IOException;
}
