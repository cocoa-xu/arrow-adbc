/**
 * <auto-generated>
 * Autogenerated by Thrift Compiler (0.17.0)
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 * </auto-generated>
 */
using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Thrift;
using Thrift.Collections;
using Thrift.Protocol;
using Thrift.Protocol.Entities;
using Thrift.Protocol.Utilities;
using Thrift.Transport;
using Thrift.Transport.Client;
using Thrift.Transport.Server;
using Thrift.Processor;


#pragma warning disable IDE0079  // remove unnecessary pragmas
#pragma warning disable IDE0017  // object init can be simplified
#pragma warning disable IDE0028  // collection init can be simplified
#pragma warning disable IDE1006  // parts of the code use IDL spelling
#pragma warning disable CA1822   // empty DeepCopy() methods still non-static
#pragma warning disable IDE0083  // pattern matching "that is not SomeType" requires net5.0 but we still support earlier versions

namespace Apache.Hive.Service.Rpc.Thrift
{

  public partial class TSparkDirectResults : TBase
  {
    private global::Apache.Hive.Service.Rpc.Thrift.TGetOperationStatusResp _operationStatus;
    private global::Apache.Hive.Service.Rpc.Thrift.TGetResultSetMetadataResp _resultSetMetadata;
    private global::Apache.Hive.Service.Rpc.Thrift.TFetchResultsResp _resultSet;
    private global::Apache.Hive.Service.Rpc.Thrift.TCloseOperationResp _closeOperation;

    public global::Apache.Hive.Service.Rpc.Thrift.TGetOperationStatusResp OperationStatus
    {
      get
      {
        return _operationStatus;
      }
      set
      {
        __isset.operationStatus = true;
        this._operationStatus = value;
      }
    }

    public global::Apache.Hive.Service.Rpc.Thrift.TGetResultSetMetadataResp ResultSetMetadata
    {
      get
      {
        return _resultSetMetadata;
      }
      set
      {
        __isset.resultSetMetadata = true;
        this._resultSetMetadata = value;
      }
    }

    public global::Apache.Hive.Service.Rpc.Thrift.TFetchResultsResp ResultSet
    {
      get
      {
        return _resultSet;
      }
      set
      {
        __isset.resultSet = true;
        this._resultSet = value;
      }
    }

    public global::Apache.Hive.Service.Rpc.Thrift.TCloseOperationResp CloseOperation
    {
      get
      {
        return _closeOperation;
      }
      set
      {
        __isset.closeOperation = true;
        this._closeOperation = value;
      }
    }


    public Isset __isset;
    public struct Isset
    {
      public bool operationStatus;
      public bool resultSetMetadata;
      public bool resultSet;
      public bool closeOperation;
    }

    public TSparkDirectResults()
    {
    }

    public TSparkDirectResults DeepCopy()
    {
      var tmp245 = new TSparkDirectResults();
      if ((OperationStatus != null) && __isset.operationStatus)
      {
        tmp245.OperationStatus = (global::Apache.Hive.Service.Rpc.Thrift.TGetOperationStatusResp)this.OperationStatus.DeepCopy();
      }
      tmp245.__isset.operationStatus = this.__isset.operationStatus;
      if ((ResultSetMetadata != null) && __isset.resultSetMetadata)
      {
        tmp245.ResultSetMetadata = (global::Apache.Hive.Service.Rpc.Thrift.TGetResultSetMetadataResp)this.ResultSetMetadata.DeepCopy();
      }
      tmp245.__isset.resultSetMetadata = this.__isset.resultSetMetadata;
      if ((ResultSet != null) && __isset.resultSet)
      {
        tmp245.ResultSet = (global::Apache.Hive.Service.Rpc.Thrift.TFetchResultsResp)this.ResultSet.DeepCopy();
      }
      tmp245.__isset.resultSet = this.__isset.resultSet;
      if ((CloseOperation != null) && __isset.closeOperation)
      {
        tmp245.CloseOperation = (global::Apache.Hive.Service.Rpc.Thrift.TCloseOperationResp)this.CloseOperation.DeepCopy();
      }
      tmp245.__isset.closeOperation = this.__isset.closeOperation;
      return tmp245;
    }

    public async global::System.Threading.Tasks.Task ReadAsync(TProtocol iprot, CancellationToken cancellationToken)
    {
      iprot.IncrementRecursionDepth();
      try
      {
        TField field;
        await iprot.ReadStructBeginAsync(cancellationToken);
        while (true)
        {
          field = await iprot.ReadFieldBeginAsync(cancellationToken);
          if (field.Type == TType.Stop)
          {
            break;
          }

          switch (field.ID)
          {
            case 1:
              if (field.Type == TType.Struct)
              {
                OperationStatus = new global::Apache.Hive.Service.Rpc.Thrift.TGetOperationStatusResp();
                await OperationStatus.ReadAsync(iprot, cancellationToken);
              }
              else
              {
                await TProtocolUtil.SkipAsync(iprot, field.Type, cancellationToken);
              }
              break;
            case 2:
              if (field.Type == TType.Struct)
              {
                ResultSetMetadata = new global::Apache.Hive.Service.Rpc.Thrift.TGetResultSetMetadataResp();
                await ResultSetMetadata.ReadAsync(iprot, cancellationToken);
              }
              else
              {
                await TProtocolUtil.SkipAsync(iprot, field.Type, cancellationToken);
              }
              break;
            case 3:
              if (field.Type == TType.Struct)
              {
                ResultSet = new global::Apache.Hive.Service.Rpc.Thrift.TFetchResultsResp();
                await ResultSet.ReadAsync(iprot, cancellationToken);
              }
              else
              {
                await TProtocolUtil.SkipAsync(iprot, field.Type, cancellationToken);
              }
              break;
            case 4:
              if (field.Type == TType.Struct)
              {
                CloseOperation = new global::Apache.Hive.Service.Rpc.Thrift.TCloseOperationResp();
                await CloseOperation.ReadAsync(iprot, cancellationToken);
              }
              else
              {
                await TProtocolUtil.SkipAsync(iprot, field.Type, cancellationToken);
              }
              break;
            default:
              await TProtocolUtil.SkipAsync(iprot, field.Type, cancellationToken);
              break;
          }

          await iprot.ReadFieldEndAsync(cancellationToken);
        }

        await iprot.ReadStructEndAsync(cancellationToken);
      }
      finally
      {
        iprot.DecrementRecursionDepth();
      }
    }

    public async global::System.Threading.Tasks.Task WriteAsync(TProtocol oprot, CancellationToken cancellationToken)
    {
      oprot.IncrementRecursionDepth();
      try
      {
        var tmp246 = new TStruct("TSparkDirectResults");
        await oprot.WriteStructBeginAsync(tmp246, cancellationToken);
        var tmp247 = new TField();
        if ((OperationStatus != null) && __isset.operationStatus)
        {
          tmp247.Name = "operationStatus";
          tmp247.Type = TType.Struct;
          tmp247.ID = 1;
          await oprot.WriteFieldBeginAsync(tmp247, cancellationToken);
          await OperationStatus.WriteAsync(oprot, cancellationToken);
          await oprot.WriteFieldEndAsync(cancellationToken);
        }
        if ((ResultSetMetadata != null) && __isset.resultSetMetadata)
        {
          tmp247.Name = "resultSetMetadata";
          tmp247.Type = TType.Struct;
          tmp247.ID = 2;
          await oprot.WriteFieldBeginAsync(tmp247, cancellationToken);
          await ResultSetMetadata.WriteAsync(oprot, cancellationToken);
          await oprot.WriteFieldEndAsync(cancellationToken);
        }
        if ((ResultSet != null) && __isset.resultSet)
        {
          tmp247.Name = "resultSet";
          tmp247.Type = TType.Struct;
          tmp247.ID = 3;
          await oprot.WriteFieldBeginAsync(tmp247, cancellationToken);
          await ResultSet.WriteAsync(oprot, cancellationToken);
          await oprot.WriteFieldEndAsync(cancellationToken);
        }
        if ((CloseOperation != null) && __isset.closeOperation)
        {
          tmp247.Name = "closeOperation";
          tmp247.Type = TType.Struct;
          tmp247.ID = 4;
          await oprot.WriteFieldBeginAsync(tmp247, cancellationToken);
          await CloseOperation.WriteAsync(oprot, cancellationToken);
          await oprot.WriteFieldEndAsync(cancellationToken);
        }
        await oprot.WriteFieldStopAsync(cancellationToken);
        await oprot.WriteStructEndAsync(cancellationToken);
      }
      finally
      {
        oprot.DecrementRecursionDepth();
      }
    }

    public override bool Equals(object that)
    {
      if (!(that is TSparkDirectResults other)) return false;
      if (ReferenceEquals(this, other)) return true;
      return ((__isset.operationStatus == other.__isset.operationStatus) && ((!__isset.operationStatus) || (global::System.Object.Equals(OperationStatus, other.OperationStatus))))
        && ((__isset.resultSetMetadata == other.__isset.resultSetMetadata) && ((!__isset.resultSetMetadata) || (global::System.Object.Equals(ResultSetMetadata, other.ResultSetMetadata))))
        && ((__isset.resultSet == other.__isset.resultSet) && ((!__isset.resultSet) || (global::System.Object.Equals(ResultSet, other.ResultSet))))
        && ((__isset.closeOperation == other.__isset.closeOperation) && ((!__isset.closeOperation) || (global::System.Object.Equals(CloseOperation, other.CloseOperation))));
    }

    public override int GetHashCode() {
      int hashcode = 157;
      unchecked {
        if ((OperationStatus != null) && __isset.operationStatus)
        {
          hashcode = (hashcode * 397) + OperationStatus.GetHashCode();
        }
        if ((ResultSetMetadata != null) && __isset.resultSetMetadata)
        {
          hashcode = (hashcode * 397) + ResultSetMetadata.GetHashCode();
        }
        if ((ResultSet != null) && __isset.resultSet)
        {
          hashcode = (hashcode * 397) + ResultSet.GetHashCode();
        }
        if ((CloseOperation != null) && __isset.closeOperation)
        {
          hashcode = (hashcode * 397) + CloseOperation.GetHashCode();
        }
      }
      return hashcode;
    }

    public override string ToString()
    {
      var tmp248 = new StringBuilder("TSparkDirectResults(");
      int tmp249 = 0;
      if ((OperationStatus != null) && __isset.operationStatus)
      {
        if (0 < tmp249++) { tmp248.Append(", "); }
        tmp248.Append("OperationStatus: ");
        OperationStatus.ToString(tmp248);
      }
      if ((ResultSetMetadata != null) && __isset.resultSetMetadata)
      {
        if (0 < tmp249++) { tmp248.Append(", "); }
        tmp248.Append("ResultSetMetadata: ");
        ResultSetMetadata.ToString(tmp248);
      }
      if ((ResultSet != null) && __isset.resultSet)
      {
        if (0 < tmp249++) { tmp248.Append(", "); }
        tmp248.Append("ResultSet: ");
        ResultSet.ToString(tmp248);
      }
      if ((CloseOperation != null) && __isset.closeOperation)
      {
        if (0 < tmp249++) { tmp248.Append(", "); }
        tmp248.Append("CloseOperation: ");
        CloseOperation.ToString(tmp248);
      }
      tmp248.Append(')');
      return tmp248.ToString();
    }
  }

}