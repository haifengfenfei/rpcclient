<?php declare(strict_types=1);


namespace Six\Rpc\Client;

use ReflectionException;
use Swoft\Bean\BeanFactory;
use Swoft\Bean\Exception\ContainerException;
use Swoft\Rpc\Client\Contract\ExtenderInterface;
use Swoft\Rpc\Client\Contract\ProviderInterface;
use Swoft\Rpc\Client\Exception\RpcClientException;
use Swoft\Rpc\Contract\PacketInterface;
use Swoft\Stdlib\Helper\Arr;

/**
 * Class Client
 *
 * @since 2.0
 */
class Client
{
    /**
     * Default host
     *
     * @var string
     */
    protected $host = '127.0.0.1';


    /**
     * Default port
     *
     * @var int
     */
    protected $port = 18307;

    /**
     * Setting
     *
     * @var array
     */
    protected $setting = [];

    /**
     * @var PacketInterface
     */
    protected $packet;

    /**
     * @var \App\Components\RpcClient\ExtenderInterface
     */
    protected $extender;

    /**
     * @var \App\Components\RpcClient\ProviderInterface
     */
    protected $provider;

    /**
     * @param Pool $pool
     *
     * @return Connection
     * @throws ReflectionException
     * @throws ContainerException
     */
    public function createConnection($pool): Connection
    {
        $connection = Connection::new($this, $pool);
        $connection->create();
        return $connection;
    }

    /**
     * @return string
     */
    public function getHost(): string
    {
        return $this->host;
    }

    /**
     * @return int
     */
    public function getPort(): int
    {
        return $this->port;
    }

    /**
     * @return array
     */
    public function getSetting(): array
    {
        return Arr::merge($this->defaultSetting(), $this->setting);
    }

    /**
     * @return PacketInterface
     * @throws RpcClientException
     */
    public function getPacket(): PacketInterface
    {
        if (empty($this->packet)) {
            throw new RpcClientException(
                sprintf('Client(%s) packet can not be null', __CLASS__)
            );
        }
        return $this->packet;
    }

    /**
     * @return ExtenderInterface
     * @throws ReflectionException
     * @throws ContainerException
     */
    public function getExtender(): Extender
    {
        if (!empty($this->extender) && $this->extender instanceof ExtenderInterface) {
            return $this->extender;
        }
        return BeanFactory::getBean('rpcClientExtender');
    }

    /**
     * @return ProviderInterface
     */
    public function getProvider(): ?\Six\Rpc\Client\Contract\ProviderInterface
    {
        return $this->provider;
    }

    /**
     * @return array
     */
    private function defaultSetting(): array
    {
        return [
            'open_eof_check' => true,
            'open_eof_split' => true,
            'package_eof'    => "\r\n\r\n",
        ];
    }
}