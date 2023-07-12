using Amazon;
using Amazon.Lambda.APIGatewayEvents;
using Amazon.Lambda.Core;
using Amazon.Lambda.SQSEvents;
using Amazon.Runtime;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using Amazon.SQS;
using Amazon.SQS.Model;
using System.Text;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace AWSLambda1;

public class Function
{
    private const string QUEUE_URL = "https://sqs.eu-north-1.amazonaws.com/530375214676/aws-task9-uploads-notification-queue";
    private const string TOPIC_ARN = "arn:aws:sns:eu-north-1:530375214676:aws-task9-uploads-notification-topic";
    private const int MAX_NUMBER_OF_MESSAGES = 10;

    private readonly IAmazonSimpleNotificationService _snsClient;
    private readonly IAmazonSQS _sqsClient;
    private ILambdaLogger _logger;

    public Function()
    {
        _snsClient = new AmazonSimpleNotificationServiceClient(RegionEndpoint.EUNorth1);
        _sqsClient = new AmazonSQSClient(RegionEndpoint.EUNorth1);
    }

    /// <summary>
    /// This method is called for every Lambda invocation. This method takes in an SQS event object and can be used 
    /// to respond to SNS messages.
    /// </summary>
    /// <param name="evnt"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    public async Task<APIGatewayProxyResponse> FunctionHandler(SQSEvent evnt, ILambdaContext context)
    {
        _logger = context.Logger;

        var messages = await ReadMessages();
        if (messages.Count > 0)
        {
            await PublishMessagesToTopicAsync(messages);
            await DeleteMessagesFromQueueAsync(messages);
        }

        _logger.LogLine($"" +
            $"Handled Request for ARN = {TOPIC_ARN}; " +
            $"Function Name = {context.FunctionName}; " +
            $"Processed Messages count = {messages.Count}; " +
            $"Remaining Time in millis = {context.RemainingTime}");

        return new APIGatewayProxyResponse
        {
            StatusCode = 200,
            Body = "",
            IsBase64Encoded = false
        };
    }

    private async Task<List<Message>> ReadMessages()
    {
        var messages = new List<Message>();
        var receiveRequest = new ReceiveMessageRequest
        {
            QueueUrl = QUEUE_URL,
            MaxNumberOfMessages = MAX_NUMBER_OF_MESSAGES,
        };

        try
        {
            for (int i = 0; i <= MAX_NUMBER_OF_MESSAGES; i++)
            {
                var response = await _sqsClient.ReceiveMessageAsync(receiveRequest);
                messages.AddRange(response.Messages);
            }

            if (!messages.Any())
            {
                _logger.LogLine("Messages not found. End processing \n");
            }

            return messages;
        }
        catch (AmazonSQSException ex)
        {
            _logger.LogLine($"Amazon queue service error while reading messages: \n{ex.Message} \n");
        }
        catch (AmazonServiceException ex)
        {
            _logger.LogLine($"Amazon service error reading messages: \n{ex.Message} \n");
        }
        catch (Exception ex)
        {
            _logger.LogLine($"Unknown error reading messages: \n{ex.Message} \n");
        }

        return messages;
    }

    private async Task PublishMessagesToTopicAsync(List<Message> messages)
    {
        var combine = CombineMessage(messages);
        var publishRequest = new PublishRequest
        {
            TopicArn = TOPIC_ARN,
            Message = combine
        };

        try
        {
            _logger.LogLine($"Result message = \n \n {combine} \n");
            await _snsClient.PublishAsync(publishRequest);
        }
        catch (AmazonSimpleNotificationServiceException ex)
        {
            _logger.LogLine($"Amazon notification service error while sending the message to topic: \n{ex.Message}\n");
        }
        catch (AmazonServiceException ex)
        {
            _logger.LogLine($"Amazon service error while sending the message to topic: \n{ex.Message}\n");
        }
        catch (Exception ex)
        {
            _logger.LogLine($"Unknown error while sending the message to topic: \n{ex.Message}\n");
        }
    }


    private async Task<int> DeleteMessagesFromQueueAsync(List<Message> messages)
    {
        try
        {
            foreach (var message in messages)
            {
                _logger.LogLine($"Deleting message id = {message.MessageId} \n");
                await _sqsClient.DeleteMessageAsync(QUEUE_URL, message.ReceiptHandle);
            }

            return messages.Count;
        }
        catch (AmazonSQSException ex)
        {
            _logger.LogLine($"Amazon queue service error while deleting messages: \n{ex.Message} \n");
        }
        catch (AmazonServiceException ex)
        {
            _logger.LogLine($"Amazon service error deleting messages: \n{ex.Message} \n");
        }
        catch (Exception ex)
        {
            _logger.LogLine($"Unknown error deleting messages: \n{ex.Message} \n");
        }

        return 0;
    }

    private string CombineMessage(List<Message> messages)
    {
        StringBuilder builder = new();

        foreach (var message in messages)
        {
            builder.AppendLine(message.Body.ToString());
            builder.AppendLine("==============================================================================================================");
        }

        return builder.ToString();
    }
}