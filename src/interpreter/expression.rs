use std::cmp::Ordering;
use std::fmt::Display;

use crate::interpreter::parser::BinaryOp;
use crate::interpreter::parser::CompareOp;
use crate::interpreter::parser::Expr;
use crate::interpreter::parser::UnaryOp;
use crate::interpreter::schema::calc_collation;
use crate::interpreter::schema::calc_type_affinity;
use crate::interpreter::schema::ColumnNumber;
use crate::interpreter::schema::Table;
use crate::interpreter::value::Buffer;
use crate::interpreter::value::Collation;
use crate::interpreter::value::ConstantValue;
use crate::interpreter::value::TypeAffinity;
use crate::interpreter::value::Value;
use crate::interpreter::value::ValueCmp;
use crate::interpreter::value::DEFAULT_COLLATION;

#[derive(Debug)]
pub enum Error {
    CollationNotFound,
    ColumnNotFound,
    NoTableContext,
    FailGetColumn(Box<dyn std::error::Error + Sync + Send>),
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::CollationNotFound => None,
            Self::ColumnNotFound => None,
            Self::NoTableContext => None,
            Self::FailGetColumn(e) => Some(e.as_ref()),
        }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CollationNotFound => {
                write!(f, "collation not found")
            }
            Self::ColumnNotFound => {
                write!(f, "column not found")
            }
            Self::NoTableContext => {
                write!(f, "no table context")
            }
            Self::FailGetColumn(e) => {
                write!(f, "fail to get column: {}", e)
            }
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
pub type ExecutionResult<'a> = Result<(
    Option<Value<'a>>,
    Option<TypeAffinity>,
    Option<(&'a Collation, CollateOrigin)>,
)>;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum CollateOrigin {
    Column,
    Expression,
}

fn filter_expression_collation(
    collation: Option<(&Collation, CollateOrigin)>,
) -> Option<(&Collation, CollateOrigin)> {
    match collation {
        Some((_, CollateOrigin::Expression)) => collation,
        _ => None,
    }
}

pub trait DataContext {
    fn get_column_value(
        &self,
        column_idx: &ColumnNumber,
    ) -> std::result::Result<Option<Value>, Box<dyn std::error::Error + Sync + Send>>;
}

#[derive(Debug, Clone)]
pub enum Expression {
    Column((ColumnNumber, TypeAffinity, Collation)),
    UnaryOperator {
        operator: UnaryOp,
        expr: Box<Expression>,
    },
    Collate {
        expr: Box<Expression>,
        collation: Collation,
    },
    BinaryOperator {
        operator: BinaryOp,
        left: Box<Expression>,
        right: Box<Expression>,
    },
    Cast {
        expr: Box<Expression>,
        type_affinity: TypeAffinity,
    },
    Null,
    Const(ConstantValue),
}

impl Expression {
    #[inline]
    pub fn one() -> Self {
        Self::Const(ConstantValue::Integer(1))
    }

    pub fn from(expr: Expr, table: Option<&Table>) -> Result<Self> {
        match expr {
            Expr::Null => Ok(Self::Null),
            Expr::Integer(i) => Ok(Self::Const(ConstantValue::Integer(i))),
            Expr::Real(f) => Ok(Self::Const(ConstantValue::Real(f))),
            Expr::Text(text) => Ok(Self::Const(ConstantValue::Text(text.dequote()))),
            Expr::Blob(hex) => Ok(Self::Const(ConstantValue::Blob(hex.decode()))),
            Expr::UnaryOperator { operator, expr } => Ok(Self::UnaryOperator {
                operator,
                expr: Box::new(Self::from(*expr, table)?),
            }),
            Expr::Collate {
                expr,
                collation_name,
            } => Ok(Self::Collate {
                expr: Box::new(Self::from(*expr, table)?),
                collation: calc_collation(&collation_name).ok_or(Error::CollationNotFound)?,
            }),
            Expr::BinaryOperator {
                operator,
                left,
                right,
            } => Ok(Self::BinaryOperator {
                operator,
                left: Box::new(Self::from(*left, table)?),
                right: Box::new(Self::from(*right, table)?),
            }),
            Expr::Column(column_name) => {
                if let Some(table) = table {
                    let column_name = column_name.dequote();
                    table
                        .get_column(&column_name)
                        .map(Self::Column)
                        .ok_or(Error::ColumnNotFound)
                } else {
                    Err(Error::NoTableContext)
                }
            }
            Expr::Cast { expr, type_name } => Ok(Self::Cast {
                expr: Box::new(Self::from(*expr, table)?),
                type_affinity: calc_type_affinity(&type_name),
            }),
        }
    }

    /// Execute the expression and return the result.
    ///
    /// TODO: The row should be a context object.
    pub fn execute<'a, D: DataContext>(&'a self, row: Option<&'a D>) -> ExecutionResult<'a> {
        match self {
            Self::Column((idx, affinity, collation)) => {
                if let Some(row) = row {
                    Ok((
                        row.get_column_value(idx).map_err(Error::FailGetColumn)?,
                        Some(*affinity),
                        Some((collation, CollateOrigin::Column)),
                    ))
                } else {
                    Err(Error::NoTableContext)
                }
            }
            Self::UnaryOperator { operator, expr } => {
                let (value, _, collation) = expr.execute(row)?;
                let value = match operator {
                    UnaryOp::BitNot => value.map(|v| Value::Integer(!v.as_integer())),
                    UnaryOp::Minus => value.map(|v| match v {
                        Value::Integer(i) => Value::Integer(-i),
                        Value::Real(d) => Value::Real(-d),
                        Value::Text(_) | Value::Blob(_) => Value::Integer(0),
                    }),
                };
                Ok((value, None, filter_expression_collation(collation)))
            }
            Self::Collate { expr, collation } => {
                let (value, affinity, _) = expr.execute(row)?;
                Ok((
                    value,
                    // Type affinity is preserved.
                    affinity,
                    Some((collation, CollateOrigin::Expression)),
                ))
            }
            Self::BinaryOperator {
                operator,
                left,
                right,
            } => {
                let (left_value, left_affinity, left_collation) = left.execute(row)?;
                let (right_value, right_affinity, right_collation) = right.execute(row)?;

                // TODO: Confirm whether collation is preserved after NULL.
                let (mut left_value, mut right_value) = match (left_value, right_value) {
                    (None, _) | (_, None) => return Ok((None, None, None)),
                    (Some(left_value), Some(right_value)) => (left_value, right_value),
                };

                let collation = match (left_collation, right_collation) {
                    (None, _) => right_collation,
                    (Some((_, CollateOrigin::Column)), Some((_, CollateOrigin::Expression))) => {
                        right_collation
                    }
                    _ => left_collation,
                };
                let next_collation = filter_expression_collation(collation);

                match operator {
                    BinaryOp::Compare(compare_op) => {
                        // Type Conversions Prior To Comparison
                        match (left_affinity, right_affinity) {
                            (
                                Some(TypeAffinity::Integer)
                                | Some(TypeAffinity::Real)
                                | Some(TypeAffinity::Numeric),
                                Some(TypeAffinity::Text) | Some(TypeAffinity::Blob) | None,
                            ) => {
                                right_value = right_value.apply_numeric_affinity();
                            }
                            (
                                Some(TypeAffinity::Text) | Some(TypeAffinity::Blob) | None,
                                Some(TypeAffinity::Integer)
                                | Some(TypeAffinity::Real)
                                | Some(TypeAffinity::Numeric),
                            ) => {
                                left_value = left_value.apply_numeric_affinity();
                            }
                            (Some(TypeAffinity::Text), None) => {
                                right_value = right_value.apply_text_affinity();
                            }
                            (None, Some(TypeAffinity::Text)) => {
                                left_value = left_value.apply_text_affinity();
                            }
                            _ => {}
                        }

                        let cmp = ValueCmp::new(
                            &left_value,
                            collation.map(|(c, _)| c).unwrap_or(&DEFAULT_COLLATION),
                        )
                        .compare(&right_value);

                        let result = match compare_op {
                            CompareOp::Eq => cmp == Ordering::Equal,
                            CompareOp::Ne => cmp != Ordering::Equal,
                            CompareOp::Lt => cmp == Ordering::Less,
                            CompareOp::Le => cmp != Ordering::Greater,
                            CompareOp::Gt => cmp == Ordering::Greater,
                            CompareOp::Ge => cmp != Ordering::Less,
                        };
                        if result {
                            Ok((Some(Value::Integer(1)), None, next_collation))
                        } else {
                            Ok((Some(Value::Integer(0)), None, next_collation))
                        }
                    }
                    BinaryOp::Concat => {
                        // Both operands are forcibly converted to text before concatination. Both
                        // are not null.
                        let left = left_value.force_text_buffer();
                        let right = right_value.force_text_buffer();
                        let mut buffer = match left {
                            Buffer::Owned(buf) => buf,
                            Buffer::Ref(buf) => {
                                let mut buffer = Vec::with_capacity(buf.len() + right.len());
                                buffer.extend(buf);
                                buffer
                            }
                        };
                        buffer.extend(right.iter());
                        Ok((
                            Some(Value::Text(Buffer::Owned(buffer))),
                            None,
                            next_collation,
                        ))
                    }
                }
            }
            Self::Cast {
                expr,
                type_affinity,
            } => {
                let (value, _affinity, collation) = expr.execute(row)?;
                Ok((
                    value.map(|v| v.force_apply_type_affinity(*type_affinity)),
                    Some(*type_affinity),
                    collation,
                ))
            }
            Self::Null => Ok((None, None, None)),
            Self::Const(value) => Ok((Some(value.as_value()), None, None)),
        }
    }
}
