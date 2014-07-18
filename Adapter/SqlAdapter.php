<?php
namespace Millwright\Semaphore\Adapter;

use Millwright\Semaphore\Model\AdapterInterface;

/**
 * Sql semaphore adapter
 */
abstract class SqlAdapter implements AdapterInterface
{
    /**
     * {@inheritDoc}
     */
    protected function deleteExpired(\DateTimeInterface $time)
    {
        $sqlDate = $time->format(\DateTime::ISO8601);
        $query   = 'DELETE FROM %table% WHERE expire_date < ?';

        $this->exec($query, array($sqlDate));
    }

    /**
     * Invalidate old locks
     */
    protected function invalidate()
    {
        $this->deleteExpired(new \DateTime('now', new \DateTimeZone('UTC')));
    }

    /**
     * Execute statement
     *
     * @param string $query
     * @param array  $arg
     */
    abstract protected function exec($query, array $args);

    /**
     * Insert record
     *
     * @param string $query
     * @param mixed  $arg
     *
     * @return boolean is inserted
     */
    abstract protected function insert($query, $arg);

    /**
     * {@inheritDoc}
     */
    public function acquire($key, $ttl)
    {
        $this->invalidate();

        $time = new \DateTime('now', new \DateTimeZone('UTC'));
        $time->add(new \DateInterval(sprintf('PT0H0M%sS', $ttl)));

        $query   = 'INSERT INTO %table% (expire_date, semaphore_key) VALUES(?, ?)';
        $sqlDate = $time->format(\DateTime::ISO8601);
        $ok = $this->insert($query, array($sqlDate, $key));

        return $ok ? $key : null;
    }

    /**
     * {@inheritDoc}
     */
    public function release($handle)
    {
        $query = 'DELETE FROM %table% WHERE semaphore_key = ?';
        $this->exec($query, array($handle));
    }
}
