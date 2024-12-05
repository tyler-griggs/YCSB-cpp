//
//  discrete_generator.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_DISCRETE_GENERATOR_H_
#define YCSB_C_DISCRETE_GENERATOR_H_

#include "generator.h"

#include <atomic>
#include <cassert>
#include <vector>
#include "utils/utils.h"

namespace ycsbc
{

  template <typename Value>
  class DiscreteGenerator : public Generator<Value>
  {
  public:
    DiscreteGenerator() : sum_(0) {}
    void AddValue(Value value, double weight);

    Value Next();
    Value Last() { return last_; }
    double GetWeight(const Value &value) const;

  private:
    std::vector<std::pair<Value, double>> values_;
    double sum_;
    std::atomic<Value> last_;
  };

  template <typename Value>
  inline void DiscreteGenerator<Value>::AddValue(Value value, double weight)
  {
    if (values_.empty())
    {
      last_ = value;
    }
    values_.push_back(std::make_pair(value, weight));
    sum_ += weight;
  }

  template <typename Value>
  inline Value DiscreteGenerator<Value>::Next()
  {
    double chooser = utils::ThreadLocalRandomDouble();

    for (auto p = values_.cbegin(); p != values_.cend(); ++p)
    {
      if (chooser < p->second / sum_)
      {
        return last_ = p->first;
      }
      chooser -= p->second / sum_;
    }

    assert(false);
    return last_;
  }

  template <typename Value>
  inline double DiscreteGenerator<Value>::GetWeight(const Value &value) const
  {
    for (const auto &pair : values_)
    {
      if (pair.first == value)
      {
        return pair.second;
      }
    }
    throw std::invalid_argument("Value not found in DiscreteGenerator");
  }

} // ycsbc

#endif // YCSB_C_DISCRETE_GENERATOR_H_
