using System;
using System.Dynamic;
using NUnit.Framework;

namespace SqlServerToCouchbase.Tests
{
    public class ScrambleSensitivePipelineTests
    {
        private ScrambleSensitivePipeline _pipeline;

        [SetUp]
        public void Setup()
        {
            _pipeline = new ScrambleSensitivePipeline("Schema", "Table", "foo");
        }

        [Test]
        public void ScrambleString()
        {
            dynamic row = new ExpandoObject();
            var originalFooValue = "this should be scrambled";
            row.foo = originalFooValue;
            var result = _pipeline.Transform(row);

            Assert.That(result.foo, Is.Not.EqualTo(originalFooValue));
        }

        [Test]
        public void ScrambleInt()
        {
            dynamic row = new ExpandoObject();
            var originalFooValue = (int)12345;
            row.foo = originalFooValue;
            var result = _pipeline.Transform(row);

            Assert.That(result.foo, Is.Not.EqualTo(originalFooValue));
        }
        
        [Test]
        public void ScrambleDecimal()
        {
            dynamic row = new ExpandoObject();
            var originalFooValue = (decimal)12345.12M;
            row.foo = originalFooValue;
            var result = _pipeline.Transform(row);

            Assert.That(result.foo, Is.Not.EqualTo(originalFooValue));
        }

        [Test]
        public void ScrambleUnknownType()
        {
            dynamic row = new ExpandoObject();
            var originalFooValue = DateTime.Now;
            row.foo = originalFooValue;
            var result = _pipeline.Transform(row);

            Assert.That(result.foo, Is.Not.EqualTo(originalFooValue));
        }

        [Test]
        public void ScrambleNothing()
        {
            dynamic row = new ExpandoObject();
            var originalValue = "unscrambled";
            row.leaveUnscrambled = originalValue;
            var result = _pipeline.Transform(row);

            Assert.That(result.leaveUnscrambled, Is.EqualTo(originalValue));
        }
    }
}