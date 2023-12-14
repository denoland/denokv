// Copyright 2023 the Deno authors. All rights reserved. MIT license.

use denokv_proto::KvValue;
use num_bigint::BigInt;
use thiserror::Error;
use v8_valueserializer::Heap;
use v8_valueserializer::ParseError;
use v8_valueserializer::Value;
use v8_valueserializer::ValueDeserializer;
use v8_valueserializer::ValueSerializer;

#[derive(Clone, Debug)]
pub enum SumOperand {
  BigInt(BigInt),
  Number(f64),
  KvU64(u64),
}

#[derive(Error, Debug)]
pub enum InvalidSumOperandError {
  #[error("invalid v8 value")]
  InvalidV8Value(#[from] ParseError),
  #[error("unsupported value type")]
  UnsupportedValueType,
  #[error("operand cannot be empty")]
  OperandCannotBeEmpty,
}

impl SumOperand {
  pub fn variant_name(&self) -> &'static str {
    match self {
      Self::BigInt(_) => "BigInt",
      Self::Number(_) => "Number",
      Self::KvU64(_) => "KvU64",
    }
  }

  pub fn parse_optional(
    value: &KvValue,
  ) -> Result<Option<Self>, InvalidSumOperandError> {
    match value {
      KvValue::V8(value) => {
        if value.is_empty() {
          return Ok(None);
        }
        let value = ValueDeserializer::default().read(value)?.0;
        Ok(Some(match value {
          Value::BigInt(x) => Self::BigInt(x),
          Value::Double(x) => Self::Number(x),
          Value::I32(x) => Self::Number(x as f64),
          Value::U32(x) => Self::Number(x as f64),
          _ => {
            return Err(InvalidSumOperandError::UnsupportedValueType);
          }
        }))
      }
      KvValue::U64(x) => Ok(Some(Self::KvU64(*x))),
      _ => Err(InvalidSumOperandError::UnsupportedValueType),
    }
  }

  pub fn parse(value: &KvValue) -> Result<Self, InvalidSumOperandError> {
    Self::parse_optional(value)?
      .ok_or(InvalidSumOperandError::OperandCannotBeEmpty)
  }

  pub fn encode(self) -> KvValue {
    match self {
      Self::BigInt(x) => KvValue::V8(
        ValueSerializer::default()
          .finish(&Heap::default(), &Value::BigInt(x))
          .unwrap(),
      ),
      Self::Number(x) => KvValue::V8(
        ValueSerializer::default()
          .finish(&Heap::default(), &Value::Double(x))
          .unwrap(),
      ),
      Self::KvU64(x) => KvValue::U64(x),
    }
  }
}
