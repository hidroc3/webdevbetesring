import { Test, TestingModule } from '@nestjs/testing';
import { AwlrStationsService } from './awlr-stations.service';

describe('AwlrStationsService', () => {
  let service: AwlrStationsService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [AwlrStationsService],
    }).compile();

    service = module.get<AwlrStationsService>(AwlrStationsService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});
