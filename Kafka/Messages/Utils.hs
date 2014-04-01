module Kafka.Messages.Utils where

enum :: (Enum a, Enum b) => a -> b
enum = toEnum . fromEnum
