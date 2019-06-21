<?php declare(strict_types=1);


namespace Six\Rpc\Client\Concern;
use ReflectionException;
use Six\Rpc\Client\Connection;
use Six\Rpc\Client\ReferenceRegister;
use Swoft\Bean\BeanFactory;
use Swoft\Bean\Exception\ContainerException;
use Swoft\Connection\Pool\Exception\ConnectionPoolException;
use Swoft\Log\Debug;
use Swoft\Rpc\Client\Exception\RpcClientException;
use Swoft\Rpc\Protocol;
use Swoft\Stdlib\Helper\JsonHelper;

/**
 * Class ServiceTrait
 *
 * @since 2.0
 */
trait ServiceTrait
{

    /**
     * @param string $interfaceClass
     * @param string $methodName
     * @param array  $params
     *
     * @return mixed
     * @throws ReflectionException
     * @throws ContainerException
     * @throws ConnectionPoolException
     * @throws RpcClientException
     */
    protected function __proxyCall(string $interfaceClass, string $methodName, array $params)
    {
        $poolName = ReferenceRegister::getPool(__CLASS__);
        $version  = ReferenceRegister::getVersion(__CLASS__);
        /* @var Pool $pool */
        $pool = BeanFactory::getBean($poolName);
        /* @var Connection $connection */
        $connection = $pool->getConnection();

        \Swoft::trigger('rpcCall',null,$connection->getHostPort());

        $connection->setRelease(true);
        $packet = $connection->getPacket();
        // Ext data
        $ext = $connection->getClient()->getExtender()->getExt();

        $protocol = Protocol::new($version, $interfaceClass, $methodName, $params, $ext);
        $data     = $packet->encode($protocol);
        $message = sprintf('Rpc call failed.interface=%s method=%s', $interfaceClass, $methodName);
        $result = $this->sendAndRecv($connection, $data, $message);

        $connection->release();
        $response = $packet->decodeResponse($result);
        if ($response->getError() !== null) {
            $code      = $response->getError()->getCode();
            $message   = $response->getError()->getMessage();
            $errorData = $response->getError()->getData();

            throw new RpcClientException(
                sprintf('Rpc call error!code=%d message=%s data=%s', $code, $message, JsonHelper::encode($errorData))
            );
        }

        return $response->getResult();

    }

    /**
     * @param Connection $connection
     * @param string     $data
     * @param string     $message
     * @param bool       $reconnect
     *
     * @return string
     * @throws RpcClientException
     * @throws ReflectionException
     * @throws ContainerException
     */
    private function sendAndRecv(Connection $connection, string $data, string $message, bool $reconnect = false): string
    {

        var_dump('正常发送数据');
        //Reconnect
        if ($reconnect) {
            var_dump('重连操作');
            $connection->reconnect();
        }
        if (!$connection->send($data)) {
            var_dump($connection->send(),'发送数据不成功');
            if ($reconnect) {
                throw new RpcClientException($message);
            }
            return $this->sendAndRecv($connection, $data, $message, true);
        }
        $result = $connection->recv();
        if ($result === false || empty($result)) {
            if ($reconnect) {
                throw new RpcClientException($message);
            }
            return $this->sendAndRecv($connection, $data, $message, true);
        }

        return $result;
    }
}